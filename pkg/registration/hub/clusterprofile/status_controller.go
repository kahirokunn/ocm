package clusterprofile

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/openshift/library-go/pkg/operator/resource/resourcemerge"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	cpv1alpha2 "sigs.k8s.io/cluster-inventory-api/apis/v1alpha2"
	cpclientset "sigs.k8s.io/cluster-inventory-api/client/clientset/versioned"
	cpinformerv1alpha2 "sigs.k8s.io/cluster-inventory-api/client/informers/externalversions/apis/v1alpha2"
	cplisterv1alpha2 "sigs.k8s.io/cluster-inventory-api/client/listers/apis/v1alpha2"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	informerv1 "open-cluster-management.io/api/client/cluster/informers/externalversions/cluster/v1"
	listerv1 "open-cluster-management.io/api/client/cluster/listers/cluster/v1"
	v1 "open-cluster-management.io/api/cluster/v1"
	v1beta2 "open-cluster-management.io/api/cluster/v1beta2"
	"open-cluster-management.io/sdk-go/pkg/basecontroller/factory"
	"open-cluster-management.io/sdk-go/pkg/patcher"

	"open-cluster-management.io/ocm/pkg/common/queue"
)

const (
	byClusterName      = "by-cluster-name"
	statusFieldManager = "ocm-clusterprofile-status"
)

// clusterProfileStatusController updates ClusterProfile status and labels from ManagedCluster
// Queue key: cluster name (e.g., "cluster1")
type clusterProfileStatusController struct {
	clusterLister         listerv1.ManagedClusterLister
	clusterProfileClient  cpclientset.Interface
	clusterProfileLister  cplisterv1alpha2.ClusterProfileLister
	clusterProfileIndexer cache.Indexer
}

// indexByClusterName is the indexer function for ClusterProfile by cluster name
func indexByClusterName(obj interface{}) ([]string, error) {
	profile, ok := obj.(*cpv1alpha2.ClusterProfile)
	if !ok {
		return []string{}, fmt.Errorf("obj is supposed to be a ClusterProfile, but is %T", obj)
	}

	// Index by the cluster-name label
	if clusterName, ok := profile.Labels[v1.ClusterNameLabelKey]; ok {
		return []string{clusterName}, nil
	}

	// Fallback: use profile name as cluster name (lifecycle controller sets Name = cluster name)
	return []string{profile.Name}, nil
}

func NewClusterProfileStatusController(
	clusterInformer informerv1.ManagedClusterInformer,
	clusterProfileClient cpclientset.Interface,
	clusterProfileInformer cpinformerv1alpha2.ClusterProfileInformer) factory.Controller {

	// Add indexer for efficient lookup of profiles by cluster name
	err := clusterProfileInformer.Informer().AddIndexers(cache.Indexers{
		byClusterName: indexByClusterName,
	})
	if err != nil {
		utilruntime.HandleError(err)
	}

	c := &clusterProfileStatusController{
		clusterLister:         clusterInformer.Lister(),
		clusterProfileClient:  clusterProfileClient,
		clusterProfileLister:  clusterProfileInformer.Lister(),
		clusterProfileIndexer: clusterProfileInformer.Informer().GetIndexer(),
	}

	return factory.New().
		WithInformersQueueKeysFunc(c.clusterToQueueKey, clusterInformer.Informer()).
		WithFilteredEventsInformersQueueKeysFunc(
			c.profileToQueueKey,
			queue.UnionFilter(
				queue.FileterByLabel(v1.ClusterNameLabelKey),
				queue.FileterByLabelKeyValue(cpv1alpha2.LabelClusterManagerKey, ClusterProfileManagerName),
			),
			clusterProfileInformer.Informer()).
		WithSync(c.sync).
		ToController("ClusterProfileStatusController")
}

func (c *clusterProfileStatusController) clusterToQueueKey(obj runtime.Object) []string {
	cluster, ok := obj.(*v1.ManagedCluster)
	if !ok {
		return nil
	}
	return []string{cluster.Name}
}

func (c *clusterProfileStatusController) profileToQueueKey(obj runtime.Object) []string {
	profile, ok := obj.(*cpv1alpha2.ClusterProfile)
	if !ok {
		return nil
	}

	// Use cluster-name label (filtering already done by WithFilteredEventsInformersQueueKeysFunc)
	if clusterName, ok := profile.Labels[v1.ClusterNameLabelKey]; ok {
		return []string{clusterName}
	}

	// Fallback: profile name = cluster name
	return []string{profile.Name}
}

func (c *clusterProfileStatusController) sync(ctx context.Context, syncCtx factory.SyncContext, key string) error {
	clusterName := key
	logger := klog.FromContext(ctx).WithValues("managedCluster", clusterName)

	logger.V(4).Info("Updating ClusterProfile status")

	cluster, err := c.clusterLister.Get(clusterName)
	if errors.IsNotFound(err) {
		logger.V(4).Info("Cluster not found, skipping status update")
		return nil
	}
	if err != nil {
		return err
	}

	// Use indexer for efficient lookup instead of listing all profiles across all namespaces
	allProfiles, err := c.getProfilesByClusterName(clusterName)
	if err != nil {
		return err
	}

	logger.V(4).Info("Found profiles to update", "count", len(allProfiles))

	updatedCount := 0
	var errs []error
	for _, profile := range allProfiles {
		if profile.Spec.ClusterManager.Name != ClusterProfileManagerName {
			continue
		}

		err := c.updateClusterProfile(ctx, profile, cluster)
		if err != nil {
			logger.Error(err, "Failed to update profile",
				"namespace", profile.Namespace, "name", profile.Name)
			errs = append(errs, fmt.Errorf("failed to update ClusterProfile %s/%s: %w", profile.Namespace, profile.Name, err))
			continue
		}
		updatedCount++
	}

	if updatedCount > 0 {
		logger.V(2).Info("Updated profiles", "count", updatedCount)
		syncCtx.Recorder().Eventf(ctx, "ClusterProfileStatusUpdated",
			"updated %d cluster profiles for cluster %s", updatedCount, clusterName)
	}

	return utilerrors.NewAggregate(errs)
}

// getProfilesByClusterName efficiently retrieves all profiles for a given cluster using the indexer
func (c *clusterProfileStatusController) getProfilesByClusterName(clusterName string) ([]*cpv1alpha2.ClusterProfile, error) {
	objs, err := c.clusterProfileIndexer.ByIndex(byClusterName, clusterName)
	if err != nil {
		return nil, err
	}

	profiles := make([]*cpv1alpha2.ClusterProfile, 0, len(objs))
	for _, obj := range objs {
		profile, ok := obj.(*cpv1alpha2.ClusterProfile)
		if !ok {
			continue
		}
		profiles = append(profiles, profile)
	}

	return profiles, nil
}

func (c *clusterProfileStatusController) updateClusterProfile(
	ctx context.Context,
	existing *cpv1alpha2.ClusterProfile,
	cluster *v1.ManagedCluster) error {

	profiles := c.clusterProfileClient.ApisV1alpha2().ClusterProfiles(existing.Namespace)
	profilePatcher := patcher.NewPatcher[
		*cpv1alpha2.ClusterProfile, cpv1alpha2.ClusterProfileSpec, cpv1alpha2.ClusterProfileStatus](profiles)
	newProfile := existing.DeepCopy()
	syncLabelsFromCluster(newProfile, cluster)
	// Sync labels independently of status ownership conflicts. The label update
	// requeues this profile with the new resource version.
	updated, err := profilePatcher.PatchLabelAnnotations(ctx, newProfile, newProfile.ObjectMeta, existing.ObjectMeta)
	if updated || err != nil {
		return err
	}

	ownedFields, err := appliedStatusFields(existing)
	if err != nil {
		return fmt.Errorf("read applied ClusterProfile status fields: %w", err)
	}
	status := statusFromCluster(existing, cluster, ownedFields)
	if ownedFields.Empty() && status.Version == nil && len(status.Properties) == 0 && len(status.Conditions) == 0 {
		return nil
	}
	payload, err := json.Marshal(map[string]any{
		"apiVersion": cpv1alpha2.GroupVersion.String(),
		"kind":       cpv1alpha2.ClusterProfileKind,
		"metadata": map[string]any{
			"name": existing.Name, "namespace": existing.Namespace,
			"uid": existing.UID, "resourceVersion": existing.ResourceVersion,
		},
		"status": status,
	})
	if err != nil {
		return err
	}
	_, err = profiles.Patch(ctx, existing.Name, types.ApplyPatchType, payload, metav1.PatchOptions{FieldManager: statusFieldManager}, "status")
	return err
}

func appliedStatusFields(profile *cpv1alpha2.ClusterProfile) (*fieldpath.Set, error) {
	fields := &fieldpath.Set{}
	for _, entry := range profile.ManagedFields {
		if entry.Manager == statusFieldManager && entry.Subresource == "status" &&
			entry.Operation == metav1.ManagedFieldsOperationApply && entry.FieldsV1 != nil {
			return fields, fields.FromJSON(entry.FieldsV1.GetRawReader())
		}
	}
	return fields, nil
}

func syncLabelsFromCluster(profile *cpv1alpha2.ClusterProfile, cluster *v1.ManagedCluster) {
	mclLabels := cluster.GetLabels()
	mclSetLabel := mclLabels[v1beta2.ClusterSetLabel]

	requiredLabels := map[string]string{
		cpv1alpha2.LabelClusterManagerKey:    ClusterProfileManagerName,
		cpv1alpha2.LabelClusterSetKey:        mclSetLabel,
		cpv1alpha2.LabelInventoryMemberIDKey: inventoryMemberID(cluster),
		// Keep the cluster-name label that lifecycle controller added
		v1.ClusterNameLabelKey: cluster.Name,
	}

	modified := false
	resourcemerge.MergeMap(&modified, &profile.Labels, requiredLabels)
}

// A claim supplies only the property's name and value. Applying a full Property
// would also send its zero lastObservedTime, which another writer may publish.
type clusterProfileStatusApply struct {
	Version    *cpv1alpha2.ClusterVersion `json:"version,omitempty"`
	Properties []v1.ManagedClusterClaim   `json:"properties,omitempty"`
	Conditions []metav1.Condition         `json:"conditions,omitempty"`
}

func statusFromCluster(profile *cpv1alpha2.ClusterProfile, cluster *v1.ManagedCluster, ownedFields *fieldpath.Set) clusterProfileStatusApply {
	status := clusterProfileStatusApply{Properties: cluster.Status.ClusterClaims}
	if version := cluster.Status.Version.Kubernetes; version != "" {
		status.Version = &cpv1alpha2.ClusterVersion{Kubernetes: version}
	}
	for _, mapping := range []struct{ source, target string }{
		{v1.ManagedClusterConditionAvailable, cpv1alpha2.ClusterConditionControlPlaneHealthy},
		{v1.ManagedClusterConditionJoined, "Joined"},
	} {
		source := meta.FindStatusCondition(cluster.Status.Conditions, mapping.source)
		current := meta.FindStatusCondition(profile.Status.Conditions, mapping.target)
		// Keep our last condition if the source disappears. A condition published
		// solely by another manager must stay out of our apply configuration.
		if current != nil {
			conditionPath := fieldpath.MakePathOrDie("status", "conditions", fieldpath.KeyByFields("type", mapping.target))
			if source != nil || ownedFields.Has(conditionPath) {
				previous := *current
				previous.ObservedGeneration = 0 // ManagedCluster does not report the ClusterProfile generation.
				status.Conditions = append(status.Conditions, previous)
			}
		}
		if source != nil {
			meta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type: mapping.target, Status: source.Status, Reason: source.Reason, Message: source.Message,
			})
		}
	}
	return status
}
