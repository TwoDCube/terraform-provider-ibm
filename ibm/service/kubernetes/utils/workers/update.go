package workers

import (
	"fmt"
	"log"
	"strings"
	"time"

	v2 "github.com/IBM-Cloud/bluemix-go/api/container/containerv2"
	"github.com/IBM-Cloud/terraform-provider-ibm/ibm/conns"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

const (
	workerNormal        = "normal"
	workerDeleteState   = "deleted"
	workerDeletePending = "deleting"
	versionUpdating     = "updating"
)

// UpdateVPCWorkers
// targetWorkerPoolID can be empty to target all worker pools
func UpdateVPCWorkers(d *schema.ResourceData, meta interface{}, clusterID string, targetWorkerPoolID string) error {
	csClient, err := meta.(conns.ClientSession).VpcContainerAPI()
	if err != nil {
		return err
	}
	targetEnv, err := getVpcClusterTargetHeader(d)
	if err != nil {
		return err
	}

	// Update the worker nodes after master node kube-version is updated.
	// workers will store the existing workers info to identify the replaced node
	workersInfo := make(map[string]int)

	updateAllWorkers := d.Get("update_all_workers").(bool)
	if updateAllWorkers || d.HasChange("patch_version") || d.HasChange("retry_patch_version") {
		workers, err := csClient.Workers().ListByWorkerPool(clusterID, targetWorkerPoolID, false, targetEnv)
		if err != nil {
			d.Set("patch_version", nil)
			return fmt.Errorf("[ERROR] Error retrieving workers for cluster: %s", err)
		}

		for index, worker := range workers {
			workersInfo[worker.ID] = index
		}
		workersCount := len(workers)

		waitForWorkerUpdate := d.Get("wait_for_worker_update").(bool)

		for _, worker := range workers {
			workerPool, err := csClient.WorkerPools().GetWorkerPool(clusterID, worker.PoolID, targetEnv)
			if err != nil {
				return fmt.Errorf("[ERROR] Error retrieving worker pool: %s", err)
			}

			// check if change is present in MAJOR.MINOR version or in PATCH version
			if worker.KubeVersion.Actual != worker.KubeVersion.Target || worker.LifeCycle.ActualOperatingSystem != workerPool.OperatingSystem {
				_, err := csClient.Workers().ReplaceWokerNode(clusterID, worker.ID, targetEnv)
				// As API returns http response 204 NO CONTENT, error raised will be exempted.
				if err != nil && !strings.Contains(err.Error(), "EmptyResponseBody") {
					d.Set("patch_version", nil)
					return fmt.Errorf("[ERROR] Error replacing the worker node from the cluster: %s", err)
				}

				if waitForWorkerUpdate {
					//1. wait for worker node to delete
					_, deleteError := waitForWorkerNodetoDelete(d, meta, targetEnv, clusterID, worker.ID)
					if deleteError != nil {
						d.Set("patch_version", nil)
						return fmt.Errorf("[ERROR] Worker node - %s is failed to replace", worker.ID)
					}

					//2. wait for new workerNode
					_, newWorkerError := waitForNewWorker(d, meta, targetEnv, workersCount, clusterID, targetWorkerPoolID)
					if newWorkerError != nil {
						d.Set("patch_version", nil)
						return fmt.Errorf("[ERROR] Failed to spawn new worker node")
					}

					//3. Get new worker node ID and update the map
					newWorkerID, index, newNodeError := getNewWorkerID(d, meta, targetEnv, workersInfo, clusterID, targetWorkerPoolID)
					if newNodeError != nil {
						d.Set("patch_version", nil)
						return fmt.Errorf("[ERROR] Unable to find the new worker node info")
					}

					delete(workersInfo, worker.ID)
					workersInfo[newWorkerID] = index

					//4. wait for the worker's version update and normal state
					_, Err := waitForVpcClusterWorkersVersionUpdate(d, meta, targetEnv, clusterID, newWorkerID)
					if Err != nil {
						d.Set("patch_version", nil)
						return fmt.Errorf(
							"[ERROR] Error waiting for cluster (%s) worker nodes kube version to be updated: %s", d.Id(), Err)
					}
				}
			}
		}
	}

	return nil
}

func getVpcClusterTargetHeader(d *schema.ResourceData) (v2.ClusterTargetHeader, error) {
	targetEnv := v2.ClusterTargetHeader{}
	var resourceGroup string
	if rg, ok := d.GetOk("resource_group_id"); ok {
		resourceGroup = rg.(string)
		targetEnv.ResourceGroup = resourceGroup
	}

	return targetEnv, nil
}

func waitForWorkerNodetoDelete(d *schema.ResourceData, meta interface{}, targetEnv v2.ClusterTargetHeader, clusterID, workerID string) (interface{}, error) {

	csClient, err := meta.(conns.ClientSession).VpcContainerAPI()
	if err != nil {
		return nil, err
	}

	deleteStateConf := &resource.StateChangeConf{
		Pending: []string{workerDeletePending},
		Target:  []string{workerDeleteState},
		Refresh: func() (interface{}, string, error) {
			worker, err := csClient.Workers().Get(clusterID, workerID, targetEnv)
			if err != nil {
				return worker, workerDeletePending, nil
			}

			if worker.LifeCycle.ActualState == "deleted" {
				return worker, workerDeleteState, nil
			}
			return worker, workerDeletePending, nil
		},
		Timeout:      d.Timeout(schema.TimeoutDelete),
		Delay:        10 * time.Second,
		MinTimeout:   5 * time.Second,
		PollInterval: 5 * time.Second,
	}
	return deleteStateConf.WaitForState()
}

func waitForNewWorker(d *schema.ResourceData, meta interface{}, targetEnv v2.ClusterTargetHeader, workersCount int, clusterID, targetWorkerPoolID string) (interface{}, error) {
	csClient, err := meta.(conns.ClientSession).VpcContainerAPI()
	if err != nil {
		return nil, err
	}

	stateConf := &resource.StateChangeConf{
		Pending: []string{"creating"},
		Target:  []string{"created"},
		Refresh: func() (interface{}, string, error) {
			workers, err := csClient.Workers().ListByWorkerPool(clusterID, targetWorkerPoolID, false, targetEnv)
			if err != nil {
				return workers, "", fmt.Errorf("[ERROR] Error in retriving the list of worker nodes")
			}
			if len(workers) == workersCount {
				return workers, "created", nil
			}
			return workers, "creating", nil
		},
		Timeout:      d.Timeout(schema.TimeoutDelete),
		Delay:        10 * time.Second,
		MinTimeout:   5 * time.Second,
		PollInterval: 5 * time.Second,
	}
	return stateConf.WaitForState()
}

func getNewWorkerID(d *schema.ResourceData, meta interface{}, targetEnv v2.ClusterTargetHeader, workersInfo map[string]int, clusterID, targetWorkerPoolID string) (string, int, error) {
	csClient, err := meta.(conns.ClientSession).VpcContainerAPI()
	if err != nil {
		return "", -1, err
	}

	workers, err := csClient.Workers().ListByWorkerPool(clusterID, targetWorkerPoolID, false, targetEnv)
	if err != nil {
		return "", -1, fmt.Errorf("[ERROR] Error in retriving the list of worker nodes")
	}

	for index, worker := range workers {
		if _, ok := workersInfo[worker.ID]; !ok {
			log.Println("[DEBUG] found new replaced node: ", worker.ID)
			return worker.ID, index, nil
		}
	}
	return "", -1, fmt.Errorf("[ERROR] no new node found")
}

// waitForVpcClusterWorkersVersionUpdate Waits for Cluster version Update
func waitForVpcClusterWorkersVersionUpdate(d *schema.ResourceData, meta interface{}, target v2.ClusterTargetHeader, clusterID, workerID string) (interface{}, error) {
	csClient, err := meta.(conns.ClientSession).VpcContainerAPI()
	if err != nil {
		return nil, err
	}

	log.Printf("Waiting for worker (%s) version to be updated.", workerID)
	stateConf := &resource.StateChangeConf{
		Pending:                   []string{"retry", versionUpdating},
		Target:                    []string{workerNormal},
		Refresh:                   vpcClusterWorkersVersionRefreshFunc(csClient.Workers(), workerID, clusterID, target),
		Timeout:                   d.Timeout(schema.TimeoutUpdate),
		Delay:                     10 * time.Second,
		MinTimeout:                10 * time.Second,
		ContinuousTargetOccurence: 3,
	}

	return stateConf.WaitForState()
}

func vpcClusterWorkersVersionRefreshFunc(client v2.Workers, workerID, clusterID string, target v2.ClusterTargetHeader) resource.StateRefreshFunc {
	return func() (interface{}, string, error) {
		worker, err := client.Get(clusterID, workerID, target)
		if err != nil {
			return nil, "retry", fmt.Errorf("[ERROR] Error retrieving worker of container vpc cluster: %s", err)
		}

		// Check active updates
		if worker.Health.State == "normal" {
			return worker, workerNormal, nil
		}
		return worker, versionUpdating, nil
	}
}
