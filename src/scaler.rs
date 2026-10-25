use std::collections::HashSet;

use anyhow::{Context, Result};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, ListParams, Patch, PatchParams};
use tracing::{debug, info, warn};

/// Pod annotation the ReplicaSet consults when picking pods to evict on
/// scale-down: pods with the lowest deletion cost are evicted first, and a
/// missing annotation counts as 0.
const POD_DELETION_COST: &str = "controller.kubernetes.io/pod-deletion-cost";

/// Deletion cost applied to busy runner pods: they only become the cheapest
/// eviction candidates after every idle pod is gone, which the scale-down
/// floor in `plan_downscale` never allows.
const BUSY_DELETION_COST: i64 = 100;

/// Deletion cost of idle runner pods — the ReplicaSet's effective default.
const IDLE_DELETION_COST: i64 = 0;

/// Kubernetes scaling operations for the runner Deployment.
pub struct Scaler {
    deployments: Api<Deployment>,
    pods: Api<Pod>,
    deployment_name: String,
}

impl Scaler {
    /// Create a new Scaler using in-cluster or kubeconfig credentials.
    pub async fn new(namespace: &str, deployment_name: &str) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("failed to create Kubernetes client (are you running in-cluster?)")?;

        let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
        let pods: Api<Pod> = Api::namespaced(client, namespace);

        Ok(Self {
            deployments,
            pods,
            deployment_name: deployment_name.to_string(),
        })
    }

    /// Get the current replica count of the runner Deployment.
    pub async fn get_replicas(&self) -> Result<i32> {
        let deploy = self
            .deployments
            .get(&self.deployment_name)
            .await
            .context("failed to get runner Deployment")?;

        Ok(deploy.spec.and_then(|s| s.replicas).unwrap_or(1))
    }

    /// Set the replica count of the runner Deployment via the Scale subresource.
    pub async fn scale(&self, replicas: i32) -> Result<()> {
        let patch = serde_json::json!({
            "spec": { "replicas": replicas }
        });

        self.deployments
            .patch_scale(
                &self.deployment_name,
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await
            .context("failed to update Deployment scale")?;

        Ok(())
    }

    /// List running pods of the runner Deployment.
    ///
    /// Returns the pod objects (metadata included) so annotations can be
    /// read and patched.
    async fn list_runner_pods(&self) -> Result<Vec<Pod>> {
        let lp = ListParams::default().labels(&format!("app={}", self.deployment_name));

        let pod_list = self
            .pods
            .list(&lp)
            .await
            .context("failed to list runner pods")?;

        let pods: Vec<Pod> = pod_list
            .items
            .into_iter()
            .filter(|pod| pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running"))
            .collect();

        Ok(pods)
    }

    /// List live pod names for the runner Deployment.
    pub async fn list_live_runner_pod_names(&self) -> Result<HashSet<String>> {
        let lp = ListParams::default().labels(&format!("app={}", self.deployment_name));

        let pod_list = self
            .pods
            .list(&lp)
            .await
            .context("failed to list live runner pods")?;

        let names = pod_list
            .items
            .into_iter()
            .filter(|pod| {
                !matches!(
                    pod.status
                        .as_ref()
                        .and_then(|status| status.phase.as_deref()),
                    Some("Succeeded") | Some("Failed")
                )
            })
            .filter_map(|pod| pod.metadata.name)
            .collect();

        Ok(names)
    }

    /// Patch the pod-deletion-cost annotation on a runner pod.
    ///
    /// Returns the updated pod so callers can verify the annotation actually
    /// took effect before asking the ReplicaSet to evict anything.
    async fn set_deletion_cost(&self, name: &str, cost: i64) -> Result<Pod> {
        let patch = serde_json::json!({
            "metadata": { "annotations": { POD_DELETION_COST: cost.to_string() } },
        });

        self.pods
            .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .context(format!("failed to set pod-deletion-cost on pod {name}"))
    }

    /// Scale down safely by making busy pods expensive to evict.
    ///
    /// 1. List all running runner pods.
    /// 2. Patch the pod-deletion-cost annotation: a high cost on busy pods,
    ///    the default (0) on idle ones.
    /// 3. Patch the Deployment down to the safe replica count. The
    ///    ReplicaSet then performs the evictions itself and, thanks to the
    ///    costs, can only ever pick idle pods — no direct pod deletion, no
    ///    replacement churn from deleting pods of a still-large Deployment.
    ///
    /// If any cost patch fails or does not take effect, the scale patch is
    /// skipped; the next poll cycle re-syncs and retries.
    pub async fn safe_scale_down(&self, target: i32, busy_runners: &HashSet<String>) -> Result<()> {
        let pods = self.list_runner_pods().await?;
        let current = pods.len() as i32;
        let busy_count = pods
            .iter()
            .filter(|pod| busy_runners.contains(pod.metadata.name.as_deref().unwrap_or_default()))
            .count();

        match plan_downscale(current, target, busy_count) {
            DownscalePlan::NoSurplus => {}
            DownscalePlan::AllBusy => {
                warn!(
                    current,
                    busy = busy_count,
                    "skip scale-down: all pods are busy"
                );
            }
            DownscalePlan::Target(new_count) => {
                // Make sure the pending evictions can only target idle pods:
                // raise the cost of busy pods, reset idle ones to the
                // default.
                for pod in &pods {
                    let Some(name) = pod.metadata.name.as_deref() else {
                        continue;
                    };
                    let desired = if busy_runners.contains(name) {
                        BUSY_DELETION_COST
                    } else {
                        IDLE_DELETION_COST
                    };
                    if pod_deletion_cost(pod) == desired {
                        continue;
                    }

                    let patched = self.set_deletion_cost(name, desired).await?;
                    if pod_deletion_cost(&patched) != desired {
                        warn!(
                            pod = name,
                            "pod-deletion-cost did not take effect, skipping scale-down"
                        );
                        return Ok(());
                    }
                }

                self.scale(new_count).await?;
                info!(new_count, "scale-down complete");

                debug!(
                    idle_count = pods.len() - busy_count,
                    busy_count, "pod partition details"
                );
            }
        }

        Ok(())
    }
}

/// What a scale-down tick should do.
#[derive(Debug, PartialEq, Eq)]
enum DownscalePlan {
    /// No surplus pods — nothing to evict.
    NoSurplus,
    /// Every pod is running a job — nothing may be evicted.
    AllBusy,
    /// Patch the Deployment down to this replica count; the pending
    /// evictions are then covered by idle pods alone.
    Target(i32),
}

/// Compute the plan for a scale-down tick.
///
/// The replica count is floored at the busy-runner count so the pending
/// evictions are always coverable from the idle pool; together with the
/// pod-deletion-cost annotation (busy pods are the most expensive to evict)
/// this keeps a running job out of harm's way.
fn plan_downscale(current: i32, target: i32, busy_count: usize) -> DownscalePlan {
    let to_remove = current - target;
    if to_remove <= 0 {
        return DownscalePlan::NoSurplus;
    }
    if busy_count as i32 >= current {
        return DownscalePlan::AllBusy;
    }

    let evictable = std::cmp::min(to_remove, current - busy_count as i32);
    DownscalePlan::Target(std::cmp::max(target, current - evictable))
}

/// Read a pod's pod-deletion-cost annotation; unparsable or missing values
/// count as the default 0.
fn pod_deletion_cost(pod: &Pod) -> i64 {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|cost| cost.get(POD_DELETION_COST))
        .and_then(|cost| cost.parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{DownscalePlan, POD_DELETION_COST, plan_downscale, pod_deletion_cost};
    use k8s_openapi::api::core::v1::Pod;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    fn runner_pod(name: &str, deletion_cost: Option<&str>) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                annotations: deletion_cost.map(|cost| {
                    BTreeMap::from([(POD_DELETION_COST.to_string(), cost.to_string())])
                }),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn deletion_cost_missing_or_unparsable_defaults_to_zero() {
        assert_eq!(pod_deletion_cost(&runner_pod("a", None)), 0);
        assert_eq!(pod_deletion_cost(&runner_pod("a", Some("garbage"))), 0);
        assert_eq!(pod_deletion_cost(&runner_pod("a", Some("100"))), 100);
    }

    #[test]
    fn downscale_skips_when_there_is_nothing_to_remove() {
        assert_eq!(plan_downscale(3, 3, 0), DownscalePlan::NoSurplus);
        assert_eq!(plan_downscale(3, 5, 0), DownscalePlan::NoSurplus);
    }

    #[test]
    fn downscale_refuses_when_every_pod_is_busy() {
        assert_eq!(plan_downscale(5, 1, 5), DownscalePlan::AllBusy);
    }

    #[test]
    fn downscale_only_commits_the_idle_pool() {
        // Plain case: the whole fleet is idle, shrink straight to target.
        assert_eq!(plan_downscale(5, 1, 0), DownscalePlan::Target(1));
        // 3 busy pods: only the 2 idle ones are evictable this tick.
        assert_eq!(plan_downscale(5, 1, 3), DownscalePlan::Target(3));
        // 4 busy pods: the single idle pod still shrinks the fleet to 4.
        assert_eq!(plan_downscale(5, 1, 4), DownscalePlan::Target(4));
    }
}
