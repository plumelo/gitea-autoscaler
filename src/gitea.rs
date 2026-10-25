use std::collections::HashSet;

use anyhow::{Context, Result};
use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue};
use serde::Deserialize;
use tracing::{debug, info};

use crate::config::{Config, GiteaAuth};

// ── Gitea API response types ───────────────────────────────────────────────

/// Matches `ActionWorkflowJobsResponse` from the Gitea OpenAPI spec.
#[derive(Debug, Deserialize)]
pub struct JobsResponse {
    #[serde(default)]
    pub jobs: Vec<Job>,
    pub total_count: u64,
}

/// Matches `ActionWorkflowJob` — only the fields we need.
#[derive(Debug, Deserialize)]
pub struct Job {
    pub id: u64,
    pub status: String,
    #[serde(default)]
    pub runner_name: String,
    #[serde(default)]
    pub name: String,
    /// `runs-on` labels required by the job (Gitea serialises the job's
    /// `RunsOn` as this `labels` array).
    #[serde(default)]
    pub labels: Vec<String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct Runner {
    #[serde(default)]
    pub busy: bool,
    #[serde(default)]
    pub id: u64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct RunnersResponse {
    #[serde(default)]
    pub runners: Vec<Runner>,
}

// ── Client ─────────────────────────────────────────────────────────────────

/// HTTP client for the Gitea admin Actions API.
pub struct GiteaClient {
    client: Client,
    base_url: String,
    /// Name prefix shared by this fleet's runner registrations
    /// (`{runner_deployment}-`), derived from the autoscaled Deployment's
    /// name. Only registrations carrying this prefix are ever swept.
    runner_prefix: String,
    /// Optional `runs-on` label restricting which jobs count toward
    /// scaling; `None` counts every job.
    job_label: Option<String>,
}

impl GiteaClient {
    /// Build a new client from application config.
    pub fn new(config: &Config) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

        match &config.gitea_auth {
            GiteaAuth::Token(token) => {
                let val = HeaderValue::from_str(&format!("token {token}"))
                    .context("invalid GITEA_TOKEN value")?;
                headers.insert(AUTHORIZATION, val);
            }
            GiteaAuth::Basic { username, password } => {
                use base64::{Engine, engine::general_purpose::STANDARD};
                let encoded = STANDARD.encode(format!("{username}:{password}"));
                let val = HeaderValue::from_str(&format!("Basic {encoded}"))
                    .context("invalid basic-auth credentials")?;
                headers.insert(AUTHORIZATION, val);
            }
        }

        let client = Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .context("failed to build HTTP client")?;

        Ok(Self {
            client,
            base_url: config.gitea_url.trim_end_matches('/').to_string(),
            runner_prefix: format!("{}-", config.runner_deployment),
            job_label: config.job_label.clone(),
        })
    }

    /// Fetch queued jobs and live in-progress jobs from the admin API.
    ///
    /// Returns `(queued_job_count, set_of_busy_live_runner_pod_names)`.
    ///
    /// When a `job_label` is configured, only jobs whose `labels` contain it
    /// are counted — jobs served by other runner fleets are invisible to the
    /// autoscaler. Without one, every job counts.
    ///
    /// In-progress jobs whose `runner_name` is not present in Kubernetes are
    /// treated as stale and ignored for scaling decisions. This makes the
    /// autoscaler resilient to jobs Gitea still shows as running after a runner
    /// has already disappeared.
    pub async fn get_jobs(
        &self,
        max_replicas: u32,
        live_runner_names: &HashSet<String>,
    ) -> Result<(u32, HashSet<String>)> {
        let mut busy_runners = HashSet::new();
        let mut queued = 0u32;
        let mut stale_in_progress = 0u32;
        let mut page: u32 = 1;
        let limit: u32 = 50;

        loop {
            let url = format!(
                "{}/api/v1/admin/actions/jobs?status=queued&status=in_progress&limit={limit}&page={page}",
                self.base_url
            );

            let resp = self
                .client
                .get(&url)
                .send()
                .await
                .context("failed to reach Gitea API")?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Gitea API returned {status}: {body}");
            }

            let data: JobsResponse = resp
                .json()
                .await
                .context("failed to deserialize Gitea jobs response")?;

            let page_total = data.total_count as u32;

            let (page_queued, page_busy, page_stale) =
                summarize_page(&data.jobs, self.job_label.as_deref(), live_runner_names);
            queued += page_queued;
            stale_in_progress += page_stale;
            busy_runners.extend(page_busy);

            // Fast exit: if busy runners already >= max, no scaling action is
            // possible (can't scale up past max, won't scale down while busy).
            if busy_runners.len() as u32 >= max_replicas {
                debug!(
                    busy = busy_runners.len(),
                    max_replicas, "busy runners at max, skipping remaining pages"
                );
                break;
            }

            // Check if we've fetched everything.
            let fetched = (page - 1) * limit + data.jobs.len() as u32;
            if fetched >= page_total || data.jobs.is_empty() {
                break;
            }
            page += 1;
        }

        debug!(
            queued,
            busy = busy_runners.len(),
            stale_in_progress,
            "job summary"
        );

        if stale_in_progress > 0 {
            info!(
                stale_in_progress,
                "ignored stale in-progress jobs on non-live runners"
            );
        }

        Ok((queued, busy_runners))
    }

    /// Delete stale runner registrations belonging to this fleet only.
    ///
    /// A registration is stale only if its name starts with the fleet prefix
    /// (`{runner_deployment}-`, i.e. it was registered by a pod of the
    /// autoscaled Deployment), Gitea reports it `offline`, it is not busy,
    /// and no live Kubernetes pod bears its name. Runners outside the fleet
    /// (e.g. persistent VM runners) are never touched.
    pub async fn cleanup_offline_runners(
        &self,
        live_runner_names: &HashSet<String>,
        busy_runners: &HashSet<String>,
    ) -> Result<u32> {
        let runners = self.list_runners().await?;
        let stale_runner_ids = select_stale_offline_runners(
            &runners,
            &self.runner_prefix,
            live_runner_names,
            busy_runners,
        );
        let stale_count = stale_runner_ids.len() as u32;

        for runner in stale_runner_ids {
            info!(runner_id = runner.id, runner = %runner.name, "deleting stale offline runner");
            self.delete_runner(runner.id).await?;
        }

        Ok(stale_count)
    }

    async fn list_runners(&self) -> Result<Vec<Runner>> {
        let url = format!("{}/api/v1/admin/actions/runners", self.base_url);

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .context("failed to reach Gitea runners API")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Gitea runners API returned {status}: {body}");
        }

        let data: RunnersResponse = resp
            .json()
            .await
            .context("failed to deserialize Gitea runners response")?;

        Ok(data.runners)
    }

    async fn delete_runner(&self, runner_id: u64) -> Result<()> {
        let url = format!("{}/api/v1/admin/actions/runners/{runner_id}", self.base_url);

        let resp = self
            .client
            .delete(&url)
            .send()
            .await
            .context("failed to reach Gitea delete runner API")?;

        if resp.status() != reqwest::StatusCode::NO_CONTENT {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Gitea delete runner API returned {status}: {body}");
        }

        Ok(())
    }
}

/// Whether `job` counts toward scaling decisions for this fleet.
///
/// With `job_label` set, only jobs whose `labels` (the `runs-on` values Gitea
/// reports for the job) contain the configured label count — the runner
/// fleet only takes jobs carrying one of its own labels, so anything else
/// is work this Deployment can never run. With no label configured, every
/// job counts (upstream behavior).
fn job_matches_label(job: &Job, job_label: Option<&str>) -> bool {
    match job_label {
        Some(label) => job.labels.iter().any(|candidate| candidate == label),
        None => true,
    }
}

/// Fold one page of fetched jobs into scaling inputs.
///
/// Returns `(queued_count, busy_live_runner_pod_names, stale_in_progress_count)`.
/// Jobs that do not match `job_label` (when configured) are skipped entirely:
/// they contribute neither to the queued count nor to the busy-runner set.
fn summarize_page(
    jobs: &[Job],
    job_label: Option<&str>,
    live_runner_names: &HashSet<String>,
) -> (u32, HashSet<String>, u32) {
    let mut queued = 0u32;
    let mut busy_runners = HashSet::new();
    let mut stale_in_progress = 0u32;

    for job in jobs {
        if !job_matches_label(job, job_label) {
            debug!(
                job_id = job.id,
                name = %job.name,
                "skipping job without the configured label"
            );
            continue;
        }

        match job.status.as_str() {
            "queued" => queued += 1,
            "in_progress" if !job.runner_name.is_empty() => {
                if live_runner_names.contains(&job.runner_name) {
                    busy_runners.insert(job.runner_name.clone());
                    debug!(
                        job_id = job.id,
                        name = %job.name,
                        runner = %job.runner_name,
                        "in-progress job on live runner"
                    );
                } else {
                    stale_in_progress += 1;
                    debug!(
                        job_id = job.id,
                        name = %job.name,
                        runner = %job.runner_name,
                        "ignoring in-progress job on non-live runner"
                    );
                }
            }
            _ => {}
        }
    }

    (queued, busy_runners, stale_in_progress)
}

/// Select runner registrations that are safe to delete.
///
/// A runner is a sweep candidate only if it belongs to the autoscaled fleet —
/// its name starts with `runner_prefix`, which Kubernetes guarantees for pods
/// (and thus hostnames/registrations) of the Deployment — and it is
/// `offline`, not busy, and missing from both the live pod set and the busy
/// runner set. Foreign runners can never match the prefix.
fn select_stale_offline_runners<'a>(
    runners: &'a [Runner],
    runner_prefix: &str,
    live_runner_names: &HashSet<String>,
    busy_runners: &HashSet<String>,
) -> Vec<&'a Runner> {
    runners
        .iter()
        .filter(|runner| runner.name.starts_with(runner_prefix))
        .filter(|runner| runner.status.eq_ignore_ascii_case("offline"))
        .filter(|runner| !runner.busy)
        .filter(|runner| !live_runner_names.contains(&runner.name))
        .filter(|runner| !busy_runners.contains(&runner.name))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Job, Runner, job_matches_label, select_stale_offline_runners, summarize_page};
    use std::collections::HashSet;

    // Fleet prefix for a Deployment named `gitea-actions-runner`: its pods —
    // and therefore its runner registrations — are always named
    // `gitea-actions-runner-…`.
    const FLEET_PREFIX: &str = "gitea-actions-runner-";

    fn runner(id: u64, name: &str, status: &str, busy: bool) -> Runner {
        Runner {
            busy,
            id,
            name: name.to_string(),
            status: status.to_string(),
        }
    }

    fn job(id: u64, name: &str, status: &str, runner_name: &str, labels: &[&str]) -> Job {
        Job {
            id,
            status: status.to_string(),
            runner_name: runner_name.to_string(),
            name: name.to_string(),
            labels: labels.iter().map(|label| label.to_string()).collect(),
        }
    }

    #[test]
    fn selects_offline_runners_missing_from_cluster() {
        let live_runner_names = HashSet::from(["gitea-actions-runner-aaa-111".to_string()]);
        let busy_runners = HashSet::new();
        let runners = vec![
            runner(1, "gitea-actions-runner-aaa-111", "offline", false),
            runner(2, "gitea-actions-runner-bbb-222", "offline", false),
        ];

        let selected =
            select_stale_offline_runners(&runners, FLEET_PREFIX, &live_runner_names, &busy_runners);

        assert_eq!(selected, vec![&runners[1]]);
    }

    #[test]
    fn skips_online_and_busy_runners() {
        let live_runner_names = HashSet::new();
        let busy_runners = HashSet::from([
            "gitea-actions-runner-ccc-333".to_string(),
            "gitea-actions-runner-ddd-444".to_string(),
        ]);
        let runners = vec![
            runner(1, "gitea-actions-runner-eee-555", "online", false),
            runner(2, "gitea-actions-runner-ccc-333", "offline", true),
            runner(3, "gitea-actions-runner-ddd-444", "offline", false),
        ];

        let selected =
            select_stale_offline_runners(&runners, FLEET_PREFIX, &live_runner_names, &busy_runners);

        assert_eq!(selected, Vec::<&Runner>::new());
    }

    #[test]
    fn matches_offline_status_case_insensitively() {
        let live_runner_names = HashSet::new();
        let busy_runners = HashSet::new();
        let runners = vec![runner(9, "gitea-actions-runner-fff-666", "OffLine", false)];

        let selected =
            select_stale_offline_runners(&runners, FLEET_PREFIX, &live_runner_names, &busy_runners);

        assert_eq!(selected, vec![&runners[0]]);
    }

    #[test]
    fn never_selects_runners_outside_the_fleet() {
        // Regression: foreign runners (persistent VM runners like pml1..pml4)
        // go offline during routine reboots and never re-register — the sweep
        // must not delete them, whatever their state.
        let live_runner_names = HashSet::new();
        let busy_runners = HashSet::new();
        let runners = vec![
            runner(1, "pml1", "offline", false),
            runner(2, "pml4", "offline", false),
        ];

        let selected =
            select_stale_offline_runners(&runners, FLEET_PREFIX, &live_runner_names, &busy_runners);

        assert_eq!(selected, Vec::<&Runner>::new());
    }

    #[test]
    fn job_labels_default_to_empty_when_the_api_omits_them() {
        let job: Job = serde_json::from_str(r#"{"id":7,"status":"queued"}"#).unwrap();

        assert!(job.labels.is_empty());
    }

    #[test]
    fn matches_only_jobs_carrying_the_configured_label() {
        let labelled = job(1, "build", "queued", "", &["k8s", "linux"]);
        let unlabelled = job(2, "test", "queued", "", &[]);

        assert!(job_matches_label(&labelled, Some("k8s")));
        assert!(!job_matches_label(&unlabelled, Some("k8s")));
        // Without a configured label every job matches (upstream behavior).
        assert!(job_matches_label(&labelled, None));
        assert!(job_matches_label(&unlabelled, None));
    }

    #[test]
    fn counts_only_jobs_matching_the_configured_label() {
        let live_runner_names = HashSet::from([
            "gitea-actions-runner-aaa-111".to_string(),
            "gitea-actions-runner-bbb-222".to_string(),
        ]);
        let jobs = vec![
            job(1, "build", "queued", "", &["k8s"]),
            job(2, "lint", "queued", "", &["linux"]),
            job(
                3,
                "package",
                "in_progress",
                "gitea-actions-runner-aaa-111",
                &["k8s"],
            ),
            job(
                4,
                "deploy",
                "in_progress",
                "gitea-actions-runner-bbb-222",
                &["linux"],
            ),
            job(
                5,
                "audit",
                "in_progress",
                "gitea-actions-runner-zzz-999",
                &["k8s"],
            ),
        ];

        let (queued, busy_runners, stale_in_progress) =
            summarize_page(&jobs, Some("k8s"), &live_runner_names);

        assert_eq!(queued, 1);
        assert_eq!(
            busy_runners,
            HashSet::from(["gitea-actions-runner-aaa-111".to_string()])
        );
        assert_eq!(stale_in_progress, 1);
    }

    #[test]
    fn counts_every_job_when_no_label_is_configured() {
        let live_runner_names = HashSet::from([
            "gitea-actions-runner-aaa-111".to_string(),
            "gitea-actions-runner-bbb-222".to_string(),
        ]);
        let jobs = vec![
            job(1, "build", "queued", "", &["k8s"]),
            job(2, "lint", "queued", "", &["linux"]),
            job(
                3,
                "package",
                "in_progress",
                "gitea-actions-runner-aaa-111",
                &["k8s"],
            ),
            job(
                4,
                "deploy",
                "in_progress",
                "gitea-actions-runner-bbb-222",
                &["linux"],
            ),
            job(
                5,
                "audit",
                "in_progress",
                "gitea-actions-runner-zzz-999",
                &["k8s"],
            ),
        ];

        let (queued, busy_runners, stale_in_progress) =
            summarize_page(&jobs, None, &live_runner_names);

        assert_eq!(queued, 2);
        assert_eq!(
            busy_runners,
            HashSet::from([
                "gitea-actions-runner-aaa-111".to_string(),
                "gitea-actions-runner-bbb-222".to_string()
            ])
        );
        assert_eq!(stale_in_progress, 1);
    }
}
