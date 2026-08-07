package scheduling

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	clusterinformerv1 "open-cluster-management.io/api/client/cluster/informers/externalversions/cluster/v1"
	clusterinformerv1beta1 "open-cluster-management.io/api/client/cluster/informers/externalversions/cluster/v1beta1"
	clusterinformerv1beta2 "open-cluster-management.io/api/client/cluster/informers/externalversions/cluster/v1beta2"
	clusterlisterv1 "open-cluster-management.io/api/client/cluster/listers/cluster/v1"
	clusterlisterv1beta2 "open-cluster-management.io/api/client/cluster/listers/cluster/v1beta2"
	clusterapiv1 "open-cluster-management.io/api/cluster/v1"
	clusterapiv1beta1 "open-cluster-management.io/api/cluster/v1beta1"
	clusterapiv1beta2 "open-cluster-management.io/api/cluster/v1beta2"
	clustersdkv1beta2 "open-cluster-management.io/sdk-go/pkg/apis/cluster/v1beta2"
)

const (
	// anyClusterSet is an index value for indexPlacementByClusterSet for placement
	// not setting any clusterset
	anyClusterSet                  = "*"
	placementsByClusterSetBinding  = "placementsByClusterSet"
	clustersetBindingsByClusterSet = "clustersetBindingsByClusterSet"
	placementsByScore              = "placementsByScore"
)

type enqueuer struct {
	logger klog.Logger
	queue  workqueue.TypedRateLimitingInterface[string]

	clusterLister            clusterlisterv1.ManagedClusterLister
	clusterSetLister         clusterlisterv1beta2.ManagedClusterSetLister
	placementIndexer         cache.Indexer
	clusterSetBindingIndexer cache.Indexer
}

func newEnqueuer(
	ctx context.Context,
	queue workqueue.TypedRateLimitingInterface[string],
	clusterInformer clusterinformerv1.ManagedClusterInformer,
	clusterSetInformer clusterinformerv1beta2.ManagedClusterSetInformer,
	placementInformer clusterinformerv1beta1.PlacementInformer,
	clusterSetBindingInformer clusterinformerv1beta2.ManagedClusterSetBindingInformer) *enqueuer {
	err := placementInformer.Informer().AddIndexers(cache.Indexers{
		placementsByScore:             indexPlacementsByScore,
		placementsByClusterSetBinding: indexPlacementByClusterSetBinding,
	})
	if err != nil {
		runtime.HandleError(err)
	}

	err = clusterSetBindingInformer.Informer().AddIndexers(cache.Indexers{
		clustersetBindingsByClusterSet: indexClusterSetBindingByClusterSet,
	})
	if err != nil {
		runtime.HandleError(err)
	}

	return &enqueuer{
		logger:                   klog.FromContext(ctx),
		queue:                    queue,
		clusterLister:            clusterInformer.Lister(),
		clusterSetLister:         clusterSetInformer.Lister(),
		placementIndexer:         placementInformer.Informer().GetIndexer(),
		clusterSetBindingIndexer: clusterSetBindingInformer.Informer().GetIndexer(),
	}
}

func (e *enqueuer) enqueuePlacement(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	e.queue.Add(newPlacementQueueKey(namespace, name))
}

func (e *enqueuer) enqueueClusterSetBinding(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	if namespace == "" || name == "" {
		runtime.HandleError(fmt.Errorf("invalid cluster set binding key %q", key))
		return
	}

	e.queue.Add(newClusterSetBindingQueueKey(namespace, name))
}

func (e *enqueuer) enqueuePlacementsByClusterSetBinding(namespace, name string) error {
	key := fmt.Sprintf("%s/%s", namespace, name)

	// enqueue all placement that ref to the binding
	objs, err := e.placementIndexer.ByIndex(placementsByClusterSetBinding, key)
	if err != nil {
		return err
	}

	anyObjs, err := e.placementIndexer.ByIndex(placementsByClusterSetBinding, fmt.Sprintf("%s/%s", namespace, anyClusterSet))
	if err != nil {
		return err
	}

	objs = append(objs, anyObjs...)

	for _, o := range objs {
		placement := o.(*clusterapiv1beta1.Placement)
		e.logger.V(4).Info("Enqueue placement because of binding", "placementNamespace", placement.Namespace, "placementName", placement.Name, "bindingKey", key)
		e.enqueuePlacement(placement)
	}

	return nil
}

func (e *enqueuer) enqueueClusterSet(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	if name == "" {
		runtime.HandleError(fmt.Errorf("invalid cluster set key %q", key))
		return
	}

	e.queue.Add(newClusterSetQueueKey(name))
}

func (e *enqueuer) enqueueClusterSetBindings(clusterSetName string) error {
	objs, err := e.clusterSetBindingIndexer.ByIndex(clustersetBindingsByClusterSet, clusterSetName)
	if err != nil {
		return err
	}

	for _, o := range objs {
		clusterSetBinding := o.(*clusterapiv1beta2.ManagedClusterSetBinding)
		e.logger.V(4).Info("Enqueue clustersetbinding because of clusterset", "clusterSetBinding", klog.KObj(clusterSetBinding), "clusterSetName", clusterSetName)
		e.enqueueClusterSetBinding(clusterSetBinding)
	}

	return nil
}

func (e *enqueuer) enqueueCluster(obj interface{}) {
	cluster, ok := obj.(*clusterapiv1.ManagedCluster)
	if !ok {
		runtime.HandleError(fmt.Errorf("obj %T is not a ManagedCluster", obj))
		return
	}

	clusterSets, err := clustersdkv1beta2.GetClusterSetsOfCluster(cluster, e.clusterSetLister)
	if err != nil {
		e.logger.V(4).Error(err, "Unable to get clusterSets of cluster", "clusterName", cluster.GetName())
		return
	}

	for _, clusterSet := range clusterSets {
		e.logger.V(4).Info("Enqueue clusterSet because of cluster", "clusterSetName", clusterSet.Name, "clusterName", cluster.Name)
		e.enqueueClusterSet(clusterSet)
	}
}

func (e *enqueuer) enqueuePlacementScore(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	if namespace == "" || name == "" {
		runtime.HandleError(fmt.Errorf("invalid placement score key %q", key))
		return
	}

	e.queue.Add(newPlacementScoreQueueKey(namespace, name))
}

func (e *enqueuer) enqueuePlacementsByScore(clusterName, scoreName string) error {
	objs, err := e.placementIndexer.ByIndex(placementsByScore, scoreName)
	if err != nil {
		return err
	}

	// filter the namespace of placement based on cluster. Find all related clustersetbinding
	// to the cluster at first. Only enqueue placement when its namespace is in the valid namespaces
	// of clustersetbindings.
	filteredBindingNamespaces := sets.NewString()
	cluster, err := e.clusterLister.Get(clusterName)
	if err != nil {
		e.logger.V(4).Error(err, "Unable to get cluster", "clusterName", clusterName)
		return nil
	}

	clusterSets, err := clustersdkv1beta2.GetClusterSetsOfCluster(cluster, e.clusterSetLister)
	if err != nil {
		e.logger.V(4).Error(err, "Unable to get clusterSets of cluster", "clusterName", cluster.GetName())
		return err
	}

	for _, clusterset := range clusterSets {
		bindingObjs, err := e.clusterSetBindingIndexer.ByIndex(clustersetBindingsByClusterSet, clusterset.Name)
		if err != nil {
			return err
		}

		for _, bindingObj := range bindingObjs {
			binding := bindingObj.(*clusterapiv1beta2.ManagedClusterSetBinding)
			filteredBindingNamespaces.Insert(binding.Namespace)
		}
	}

	for _, o := range objs {
		placement := o.(*clusterapiv1beta1.Placement)
		if filteredBindingNamespaces.Has(placement.Namespace) {
			e.logger.V(4).Info("Enqueue placement because of score", "placementNamespace", placement.Namespace, "placementName", placement.Name, "clusterName", clusterName, "scoreName", scoreName)
			e.enqueuePlacement(placement)
		}
	}

	return nil
}

func indexPlacementByClusterSetBinding(obj interface{}) ([]string, error) {
	placement, ok := obj.(*clusterapiv1beta1.Placement)
	if !ok {
		return []string{}, fmt.Errorf("obj %T is not a Placement", obj)
	}

	clustersets := placement.Spec.ClusterSets
	if len(clustersets) == 0 {
		return []string{fmt.Sprintf("%s/%s", placement.Namespace, anyClusterSet)}, nil
	}

	var bindings []string
	for _, clusterset := range clustersets {
		bindings = append(bindings, fmt.Sprintf("%s/%s", placement.Namespace, clusterset))
	}

	return bindings, nil
}

func indexPlacementsByScore(obj interface{}) ([]string, error) {
	placement, ok := obj.(*clusterapiv1beta1.Placement)
	if !ok {
		return []string{}, fmt.Errorf("obj %T is not a Placement", obj)
	}

	var keys []string
	for _, config := range placement.Spec.PrioritizerPolicy.Configurations {
		if config.ScoreCoordinate == nil {
			continue
		}
		if config.ScoreCoordinate.Type != clusterapiv1beta1.ScoreCoordinateTypeAddOn {
			continue
		}
		if config.ScoreCoordinate.AddOn == nil {
			continue
		}
		keys = append(keys, config.ScoreCoordinate.AddOn.ResourceName)
	}

	return keys, nil
}

func indexClusterSetBindingByClusterSet(obj interface{}) ([]string, error) {
	binding, ok := obj.(*clusterapiv1beta2.ManagedClusterSetBinding)
	if !ok {
		return []string{}, fmt.Errorf("obj %T is not a ManagedClusterSetBinding", obj)
	}

	return []string{binding.Spec.ClusterSet}, nil
}
