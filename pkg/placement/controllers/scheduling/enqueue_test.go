package scheduling

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2/ktesting"

	clusterclient "open-cluster-management.io/api/client/cluster/clientset/versioned"
	clusterfake "open-cluster-management.io/api/client/cluster/clientset/versioned/fake"
	clusterinformers "open-cluster-management.io/api/client/cluster/informers/externalversions"
	clusterapiv1 "open-cluster-management.io/api/cluster/v1"
	clusterapiv1alpha1 "open-cluster-management.io/api/cluster/v1alpha1"
	clusterapiv1beta1 "open-cluster-management.io/api/cluster/v1beta1"
	clusterapiv1beta2 "open-cluster-management.io/api/cluster/v1beta2"

	testingcommon "open-cluster-management.io/ocm/pkg/common/testing"
	testinghelpers "open-cluster-management.io/ocm/pkg/placement/helpers/testing"
)

func newClusterInformerFactory(t *testing.T, clusterClient clusterclient.Interface, objects ...runtime.Object) clusterinformers.SharedInformerFactory {
	clusterInformerFactory := clusterinformers.NewSharedInformerFactory(clusterClient, time.Minute*10)

	err := clusterInformerFactory.Cluster().V1beta1().Placements().Informer().AddIndexers(cache.Indexers{
		placementsByScore:             indexPlacementsByScore,
		placementsByClusterSetBinding: indexPlacementByClusterSetBinding,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = clusterInformerFactory.Cluster().V1beta2().ManagedClusterSetBindings().Informer().AddIndexers(cache.Indexers{
		clustersetBindingsByClusterSet: indexClusterSetBindingByClusterSet,
	})
	if err != nil {
		t.Fatal(err)
	}

	clusterStore := clusterInformerFactory.Cluster().V1().ManagedClusters().Informer().GetStore()
	clusterSetStore := clusterInformerFactory.Cluster().V1beta2().ManagedClusterSets().Informer().GetStore()
	clusterSetBindingStore := clusterInformerFactory.Cluster().V1beta2().ManagedClusterSetBindings().Informer().GetStore()
	placementStore := clusterInformerFactory.Cluster().V1beta1().Placements().Informer().GetStore()
	placementDecisionStore := clusterInformerFactory.Cluster().V1beta1().PlacementDecisions().Informer().GetStore()
	addOnPlacementStore := clusterInformerFactory.Cluster().V1alpha1().AddOnPlacementScores().Informer().GetStore()

	for _, obj := range objects {
		var err error
		switch obj.(type) {
		case *clusterapiv1.ManagedCluster:
			err = clusterStore.Add(obj)
		case *clusterapiv1beta2.ManagedClusterSet:
			err = clusterSetStore.Add(obj)
		case *clusterapiv1beta2.ManagedClusterSetBinding:
			err = clusterSetBindingStore.Add(obj)
		case *clusterapiv1beta1.Placement:
			err = placementStore.Add(obj)
		case *clusterapiv1beta1.PlacementDecision:
			err = placementDecisionStore.Add(obj)
		case *clusterapiv1alpha1.AddOnPlacementScore:
			err = addOnPlacementStore.Add(obj)
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	return clusterInformerFactory
}

func drainPlacementQueue(t *testing.T, q *enqueuer) sets.String {
	t.Helper()

	queuedPlacements := sets.NewString()
	for q.queue.Len() > 0 {
		key, shutdown := q.queue.Get()
		if shutdown {
			t.Fatal("work queue unexpectedly shut down")
		}

		parsedKey, err := parseSchedulingQueueKey(key)
		if err != nil {
			t.Fatal(err)
		}

		switch parsedKey.keyType {
		case clusterSetQueueKeyType:
			err = q.enqueueClusterSetBindings(parsedKey.name)
		case clusterSetBindingQueueKeyType:
			err = q.enqueuePlacementsByClusterSetBinding(parsedKey.namespace, parsedKey.name)
		case placementScoreQueueKeyType:
			err = q.enqueuePlacementsByScore(parsedKey.namespace, parsedKey.name)
		case placementQueueKeyType:
			queuedPlacements.Insert(parsedKey.namespace + "/" + parsedKey.name)
		}
		q.queue.Forget(key)
		q.queue.Done(key)
		if err != nil {
			t.Fatal(err)
		}
	}

	return queuedPlacements
}

func TestEnqueuePlacementsByClusterSet(t *testing.T) {
	cases := []struct {
		name       string
		clusterSet interface{}
		initObjs   []runtime.Object
		queuedKeys []string
	}{
		{
			name:       "enqueue placements in a namespace",
			clusterSet: testinghelpers.NewClusterSet("clusterset1").Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet("clusterset1").Build(),
				testinghelpers.NewClusterSet("clusterset2").Build(),
				testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
				testinghelpers.NewClusterSetBinding("ns1", "clusterset2"),
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
				testinghelpers.NewPlacement("ns1", "placement2").WithClusterSets("clusterset2").Build(),
			},
			queuedKeys: []string{
				"ns1/placement1",
			},
		},
		{
			name:       "invalid object type",
			clusterSet: "invalid object type",
		},
		{
			name:       "clusterset selector type is LegacyClusterSetLabel",
			clusterSet: testinghelpers.NewClusterSet("clusterset1").Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
			},
			queuedKeys: []string{
				"ns1/placement1",
			},
		},
		{
			name: "clusterset selector type is LabelSelector",
			clusterSet: testinghelpers.NewClusterSet("clusterset1").WithClusterSelector(clusterapiv1beta2.ManagedClusterSelector{
				SelectorType: clusterapiv1beta2.LabelSelector,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"cloud": "Amazon",
					},
				},
			}).Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
			},
			queuedKeys: []string{
				"ns1/placement1",
			},
		},
		{
			name:       "clusterset selector type is LegacyClusterSetLabel",
			clusterSet: testinghelpers.NewClusterSet("clusterset1").Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
			},
			queuedKeys: []string{
				"ns1/placement1",
			},
		},
		{
			name: "clusterset selector type is LabelSelector",
			clusterSet: testinghelpers.NewClusterSet("clusterset1").WithClusterSelector(clusterapiv1beta2.ManagedClusterSelector{
				SelectorType: clusterapiv1beta2.LabelSelector,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"cloud": "Amazon",
					},
				},
			}).Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
			},
			queuedKeys: []string{
				"ns1/placement1",
			},
		},
		{
			name: "tombstone",
			clusterSet: cache.DeletedFinalStateUnknown{
				Key: "clusterset1",
				Obj: testinghelpers.NewClusterSet("clusterset1").Build(),
			},
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
			},
			queuedKeys: []string{
				"ns1/placement1",
			},
		},
		{
			name: "tombstone with invalid object type",
			clusterSet: cache.DeletedFinalStateUnknown{
				Obj: "invalid object type",
			},
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			clusterClient := clusterfake.NewSimpleClientset(c.initObjs...)
			clusterInformerFactory := newClusterInformerFactory(t, clusterClient, c.initObjs...)

			syncCtx := testingcommon.NewFakeSyncContext(t, "fake")
			q := newEnqueuer(
				ctx,
				syncCtx.Queue(),
				clusterInformerFactory.Cluster().V1().ManagedClusters(),
				clusterInformerFactory.Cluster().V1beta2().ManagedClusterSets(),
				clusterInformerFactory.Cluster().V1beta1().Placements(),
				clusterInformerFactory.Cluster().V1beta2().ManagedClusterSetBindings(),
			)
			q.enqueueClusterSet(c.clusterSet)
			queuedKeys := drainPlacementQueue(t, q)

			expectedQueuedKeys := sets.NewString(c.queuedKeys...)
			if !queuedKeys.Equal(expectedQueuedKeys) {
				t.Errorf("expected queued placements %q, but got %s", strings.Join(expectedQueuedKeys.List(), ","), strings.Join(queuedKeys.List(), ","))
			}
		})
	}
}

func TestEnqueuePlacementsByClusterSetBinding(t *testing.T) {
	cases := []struct {
		name              string
		namespace         string
		clusterSetBinding interface{}
		initObjs          []runtime.Object
		queuedKeys        []string
	}{
		{
			name:              "enqueue placements by clusterSetBinding",
			namespace:         "ns1",
			clusterSetBinding: testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
			initObjs: []runtime.Object{
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
				testinghelpers.NewPlacement("ns1", "placement2").WithClusterSets("clusterset2").Build(),
				testinghelpers.NewPlacement("ns2", "placement3").WithClusterSets("clusterset1").Build(),
			},
			queuedKeys: []string{
				"ns1/placement1",
			},
		},
		{
			name:              "invalid resource type",
			clusterSetBinding: "invalid resource type",
			initObjs: []runtime.Object{
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
				testinghelpers.NewPlacement("ns2", "placement2").Build(),
			},
		},
		{
			name:              "on clustersetbinding change",
			clusterSetBinding: testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet("clusterset1").Build(),
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
				testinghelpers.NewPlacement("ns2", "placement2").Build(),
			},
			queuedKeys: []string{
				"ns1/placement1",
			},
		},
		{
			name:              "clusterset",
			clusterSetBinding: testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet("clusterset1").Build(),
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
			},
			queuedKeys: []string{
				"ns1/placement1",
			},
		},
		{
			name: "tombstone",
			clusterSetBinding: cache.DeletedFinalStateUnknown{
				Key: "ns1/clusterset1",
				Obj: testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
			},
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet("clusterset1").Build(),
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
			},
			queuedKeys: []string{
				"ns1/placement1",
			},
		},
		{
			name: "tombstone with invalid object type",
			clusterSetBinding: cache.DeletedFinalStateUnknown{
				Obj: "invalid object type",
			},
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet("clusterset1").Build(),
				testinghelpers.NewPlacement("ns1", "placement1").Build(),
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			clusterClient := clusterfake.NewSimpleClientset(c.initObjs...)
			clusterInformerFactory := newClusterInformerFactory(t, clusterClient, c.initObjs...)

			syncCtx := testingcommon.NewFakeSyncContext(t, "fake")
			q := newEnqueuer(
				ctx,
				syncCtx.Queue(),
				clusterInformerFactory.Cluster().V1().ManagedClusters(),
				clusterInformerFactory.Cluster().V1beta2().ManagedClusterSets(),
				clusterInformerFactory.Cluster().V1beta1().Placements(),
				clusterInformerFactory.Cluster().V1beta2().ManagedClusterSetBindings(),
			)
			q.enqueueClusterSetBinding(c.clusterSetBinding)
			queuedKeys := drainPlacementQueue(t, q)

			expectedQueuedKeys := sets.NewString(c.queuedKeys...)
			if !queuedKeys.Equal(expectedQueuedKeys) {
				t.Errorf("expected queued placements %q, but got %s", strings.Join(expectedQueuedKeys.List(), ","), strings.Join(queuedKeys.List(), ","))
			}
		})
	}
}

func TestEnqueuePlacementsByScore(t *testing.T) {
	cases := []struct {
		name       string
		namespace  string
		score      interface{}
		initObjs   []runtime.Object
		queuedKeys []string
	}{
		{
			name:  "ensueue score",
			score: testinghelpers.NewAddOnPlacementScore("cluster1", "score1").Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewPlacement("ns1", "placement1").WithScoreCoordinateAddOn("score1", "cpu", 1).Build(),
				testinghelpers.NewPlacement("ns2", "placement2").WithScoreCoordinateAddOn("score2", "cpu", 1).Build(),
				testinghelpers.NewPlacement("ns3", "placement3").WithScoreCoordinateAddOn("score1", "cpu", 1).Build(),
				testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterapiv1beta2.ClusterSetLabel, "clusterset1").Build(),
				testinghelpers.NewClusterSet("clusterset1").Build(),
				testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
				testinghelpers.NewClusterSetBinding("ns3", "clusterset1"),
			},
			queuedKeys: []string{
				"ns1/placement1",
				"ns3/placement3",
			},
		},
		{
			name:  "only enqueue score with filtered placement",
			score: testinghelpers.NewAddOnPlacementScore("cluster1", "score1").Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewPlacement("ns1", "placement1").WithScoreCoordinateAddOn("score1", "cpu", 1).Build(),
				testinghelpers.NewPlacement("ns2", "placement2").WithScoreCoordinateAddOn("score2", "cpu", 1).Build(),
				testinghelpers.NewPlacement("ns3", "placement3").WithScoreCoordinateAddOn("score1", "cpu", 1).Build(),
				testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterapiv1beta2.ClusterSetLabel, "clusterset1").Build(),
				testinghelpers.NewClusterSet("clusterset1").Build(),
				testinghelpers.NewClusterSetBinding("ns1", "clusterset2"),
				testinghelpers.NewClusterSetBinding("ns3", "clusterset1"),
			},
			queuedKeys: []string{
				"ns3/placement3",
			},
		},
		{
			name: "tombstone",
			score: cache.DeletedFinalStateUnknown{
				Key: "cluster1/score1",
				Obj: testinghelpers.NewAddOnPlacementScore("cluster1", "score1").Build(),
			},
			initObjs: []runtime.Object{
				testinghelpers.NewPlacement("ns1", "placement1").WithScoreCoordinateAddOn("score1", "cpu", 1).Build(),
				testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterapiv1beta2.ClusterSetLabel, "clusterset1").Build(),
				testinghelpers.NewClusterSet("clusterset1").Build(),
				testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
			},
			queuedKeys: []string{
				"ns1/placement1",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			clusterClient := clusterfake.NewSimpleClientset(c.initObjs...)
			clusterInformerFactory := newClusterInformerFactory(t, clusterClient, c.initObjs...)

			syncCtx := testingcommon.NewFakeSyncContext(t, "fake")
			q := newEnqueuer(
				ctx,
				syncCtx.Queue(),
				clusterInformerFactory.Cluster().V1().ManagedClusters(),
				clusterInformerFactory.Cluster().V1beta2().ManagedClusterSets(),
				clusterInformerFactory.Cluster().V1beta1().Placements(),
				clusterInformerFactory.Cluster().V1beta2().ManagedClusterSetBindings(),
			)
			q.enqueuePlacementScore(c.score)
			queuedKeys := drainPlacementQueue(t, q)

			expectedQueuedKeys := sets.NewString(c.queuedKeys...)
			if !queuedKeys.Equal(expectedQueuedKeys) {
				t.Errorf("expected queued placements %q, but got %s", strings.Join(expectedQueuedKeys.List(), ","), strings.Join(queuedKeys.List(), ","))
			}
		})
	}
}

func TestDependencyEventsAreResolvedFromCurrentCaches(t *testing.T) {
	tests := []struct {
		name         string
		enqueueEvent func(*enqueuer)
	}{
		{
			name: "cluster set event before binding and placement caches",
			enqueueEvent: func(q *enqueuer) {
				q.enqueueClusterSet(testinghelpers.NewClusterSet("clusterset1").Build())
			},
		},
		{
			name: "binding event before placement cache",
			enqueueEvent: func(q *enqueuer) {
				q.enqueueClusterSetBinding(testinghelpers.NewClusterSetBinding("ns1", "clusterset1"))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			clusterClient := clusterfake.NewSimpleClientset()
			clusterInformerFactory := newClusterInformerFactory(t, clusterClient)
			syncCtx := testingcommon.NewFakeSyncContext(t, "fake")
			q := newEnqueuer(
				ctx,
				syncCtx.Queue(),
				clusterInformerFactory.Cluster().V1().ManagedClusters(),
				clusterInformerFactory.Cluster().V1beta2().ManagedClusterSets(),
				clusterInformerFactory.Cluster().V1beta1().Placements(),
				clusterInformerFactory.Cluster().V1beta2().ManagedClusterSetBindings(),
			)

			// Simulate an informer callback running before the related informer caches
			// contain the downstream objects. The queued dependency must be resolved
			// later from the current cache contents.
			test.enqueueEvent(q)
			if err := clusterInformerFactory.Cluster().V1beta2().ManagedClusterSetBindings().Informer().GetStore().Add(
				testinghelpers.NewClusterSetBinding("ns1", "clusterset1")); err != nil {
				t.Fatal(err)
			}
			if err := clusterInformerFactory.Cluster().V1beta1().Placements().Informer().GetStore().Add(
				testinghelpers.NewPlacement("ns1", "placement1").Build()); err != nil {
				t.Fatal(err)
			}

			actual := drainPlacementQueue(t, q)
			expected := sets.NewString("ns1/placement1")
			if !actual.Equal(expected) {
				t.Errorf("expected queued placements %v, got %v", expected.List(), actual.List())
			}
		})
	}
}

func TestSchedulingControllerDispatchesDependencyQueueKeys(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	objects := []runtime.Object{
		testinghelpers.NewClusterSetBinding("ns1", "clusterset1"),
		testinghelpers.NewPlacement("ns1", "placement1").Build(),
	}
	clusterClient := clusterfake.NewSimpleClientset(objects...)
	clusterInformerFactory := newClusterInformerFactory(t, clusterClient, objects...)
	syncCtx := testingcommon.NewFakeSyncContext(t, "fake")
	q := newEnqueuer(
		ctx,
		syncCtx.Queue(),
		clusterInformerFactory.Cluster().V1().ManagedClusters(),
		clusterInformerFactory.Cluster().V1beta2().ManagedClusterSets(),
		clusterInformerFactory.Cluster().V1beta1().Placements(),
		clusterInformerFactory.Cluster().V1beta2().ManagedClusterSetBindings(),
	)
	controller := &schedulingController{enqueuer: q}

	if err := controller.sync(ctx, syncCtx, newClusterSetQueueKey("clusterset1")); err != nil {
		t.Fatal(err)
	}
	bindingKey, shutdown := q.queue.Get()
	if shutdown {
		t.Fatal("work queue unexpectedly shut down")
	}
	q.queue.Forget(bindingKey)
	q.queue.Done(bindingKey)
	if bindingKey != newClusterSetBindingQueueKey("ns1", "clusterset1") {
		t.Fatalf("expected cluster set binding key, got %q", bindingKey)
	}

	if err := controller.sync(ctx, syncCtx, bindingKey); err != nil {
		t.Fatal(err)
	}
	placementKey, shutdown := q.queue.Get()
	if shutdown {
		t.Fatal("work queue unexpectedly shut down")
	}
	q.queue.Forget(placementKey)
	q.queue.Done(placementKey)
	if placementKey != newPlacementQueueKey("ns1", "placement1") {
		t.Fatalf("expected placement key, got %q", placementKey)
	}
}
