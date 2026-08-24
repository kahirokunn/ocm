package sigplacementdecision

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
	cpv1alpha1 "sigs.k8s.io/cluster-inventory-api/apis/v1alpha1"
	cpfake "sigs.k8s.io/cluster-inventory-api/client/clientset/versioned/fake"
	cpinformers "sigs.k8s.io/cluster-inventory-api/client/informers/externalversions"

	clusterfake "open-cluster-management.io/api/client/cluster/clientset/versioned/fake"
	clusterinformers "open-cluster-management.io/api/client/cluster/informers/externalversions"
	v1beta1 "open-cluster-management.io/api/cluster/v1beta1"

	testingcommon "open-cluster-management.io/ocm/pkg/common/testing"
)

func newOCMPlacementDecision(namespace, name, placementName string, groupIndex string, decisions []v1beta1.ClusterDecision) *v1beta1.PlacementDecision {
	labels := map[string]string{}
	if placementName != "" {
		labels[v1beta1.PlacementLabel] = placementName
	}
	if groupIndex != "" {
		labels[v1beta1.DecisionGroupIndexLabel] = groupIndex
	}

	return &v1beta1.PlacementDecision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Status: v1beta1.PlacementDecisionStatus{
			Decisions: decisions,
		},
	}
}

func newSIGPlacementDecision(namespace, name string, labels map[string]string, decisions []cpv1alpha1.ClusterDecision, schedulerName string) *cpv1alpha1.PlacementDecision {
	return newSIGPlacementDecisionWithOwnerRefs(namespace, name, labels, nil, decisions, schedulerName)
}

func newSIGPlacementDecisionWithOwnerRefs(namespace, name string, labels map[string]string,
	ownerRefs []metav1.OwnerReference, decisions []cpv1alpha1.ClusterDecision, schedulerName string) *cpv1alpha1.PlacementDecision {
	return &cpv1alpha1.PlacementDecision{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			Labels:          labels,
			OwnerReferences: ownerRefs,
		},
		Decisions:     decisions,
		SchedulerName: schedulerName,
	}
}

func ownerRefFor(ocmPD *v1beta1.PlacementDecision) metav1.OwnerReference {
	return *metav1.NewControllerRef(ocmPD, v1beta1.SchemeGroupVersion.WithKind("PlacementDecision"))
}

func TestSIGPlacementDecisionControllerSync(t *testing.T) {
	cases := []struct {
		name            string
		key             string
		ocmPDs          []runtime.Object
		existingSIGPDs  []runtime.Object
		expectedCreates int
		expectedUpdates int
		expectedDeletes int
		validateActions func(t *testing.T, actions []clienttesting.Action)
	}{
		{
			name: "create SIG PD from OCM PD with decisions",
			key:  "ns1/my-placement-decision-1",
			ocmPDs: []runtime.Object{
				newOCMPlacementDecision("ns1", "my-placement-decision-1", "my-placement", "0", []v1beta1.ClusterDecision{
					{ClusterName: "cluster1", Reason: "score"},
					{ClusterName: "cluster2", Reason: "affinity"},
				}),
			},
			existingSIGPDs:  nil,
			expectedCreates: 1,
			validateActions: func(t *testing.T, actions []clienttesting.Action) {
				created := actions[0].(clienttesting.CreateAction).GetObject().(*cpv1alpha1.PlacementDecision)
				if created.Name != "my-placement-decision-1" {
					t.Errorf("expected name my-placement-decision-1, got %s", created.Name)
				}
				if created.Namespace != "ns1" {
					t.Errorf("expected namespace ns1, got %s", created.Namespace)
				}
				if created.SchedulerName != SchedulerName {
					t.Errorf("expected scheduler %s, got %s", SchedulerName, created.SchedulerName)
				}
				if len(created.Decisions) != 2 {
					t.Fatalf("expected 2 decisions, got %d", len(created.Decisions))
				}
				if created.Decisions[0].ClusterProfileRef.Name != "cluster1" {
					t.Errorf("expected cluster1, got %s", created.Decisions[0].ClusterProfileRef.Name)
				}
				if created.Decisions[0].Reason != "score" {
					t.Errorf("expected reason 'score', got %s", created.Decisions[0].Reason)
				}
				if created.Decisions[1].ClusterProfileRef.Name != "cluster2" {
					t.Errorf("expected cluster2, got %s", created.Decisions[1].ClusterProfileRef.Name)
				}
				if created.Labels[cpv1alpha1.LabelClusterManagerKey] != SchedulerName {
					t.Errorf("expected cluster-manager label %s, got %s", SchedulerName, created.Labels[cpv1alpha1.LabelClusterManagerKey])
				}
				if created.Labels[cpv1alpha1.PlacementKeyLabel] != "my-placement" {
					t.Errorf("expected placement-key label 'my-placement', got %s", created.Labels[cpv1alpha1.PlacementKeyLabel])
				}
				if created.Labels[cpv1alpha1.DecisionKeyLabel] != "my-placement" {
					t.Errorf("expected decision-key label 'my-placement', got %s", created.Labels[cpv1alpha1.DecisionKeyLabel])
				}
				if created.Labels[cpv1alpha1.DecisionIndexLabel] != "0" {
					t.Errorf("expected decision-index label '0', got %s", created.Labels[cpv1alpha1.DecisionIndexLabel])
				}
				if created.Labels[decisionGroupIndexLabel] != "0" {
					t.Errorf("expected decision-group-index label '0', got %s", created.Labels[decisionGroupIndexLabel])
				}
				if len(created.OwnerReferences) != 1 {
					t.Fatalf("expected 1 owner reference, got %d", len(created.OwnerReferences))
				}
				ownerRef := created.OwnerReferences[0]
				if ownerRef.Name != "my-placement-decision-1" || ownerRef.Kind != "PlacementDecision" {
					t.Errorf("expected owner reference to point to OCM PlacementDecision my-placement-decision-1, got %+v", ownerRef)
				}
				if ownerRef.Controller == nil || !*ownerRef.Controller {
					t.Errorf("expected owner reference to be a controller reference")
				}
			},
		},
		{
			name: "create SIG PD with empty decisions",
			key:  "ns1/pd-empty",
			ocmPDs: []runtime.Object{
				newOCMPlacementDecision("ns1", "pd-empty", "placement-1", "0", nil),
			},
			existingSIGPDs:  nil,
			expectedCreates: 1,
			validateActions: func(t *testing.T, actions []clienttesting.Action) {
				created := actions[0].(clienttesting.CreateAction).GetObject().(*cpv1alpha1.PlacementDecision)
				if len(created.Decisions) != 0 {
					t.Errorf("expected 0 decisions, got %d", len(created.Decisions))
				}
			},
		},
		{
			name:   "no-op when OCM PD is removed since garbage collection handles cleanup",
			key:    "ns1/pd-deleted",
			ocmPDs: nil,
			existingSIGPDs: []runtime.Object{
				newSIGPlacementDecision("ns1", "pd-deleted", map[string]string{
					cpv1alpha1.LabelClusterManagerKey: SchedulerName,
				}, []cpv1alpha1.ClusterDecision{
					{ClusterProfileRef: cpv1alpha1.ClusterProfileReference{Name: "cluster1"}},
				}, SchedulerName),
			},
			expectedDeletes: 0,
			expectedUpdates: 0,
			expectedCreates: 0,
		},
		{
			name: "update SIG PD when decisions change",
			key:  "ns1/pd-update",
			ocmPDs: []runtime.Object{
				newOCMPlacementDecision("ns1", "pd-update", "my-placement", "0", []v1beta1.ClusterDecision{
					{ClusterName: "cluster1", Reason: "score"},
					{ClusterName: "cluster3", Reason: "new"},
				}),
			},
			existingSIGPDs: []runtime.Object{
				newSIGPlacementDecisionWithOwnerRefs("ns1", "pd-update", map[string]string{
					cpv1alpha1.LabelClusterManagerKey: SchedulerName,
					cpv1alpha1.PlacementKeyLabel:      "my-placement",
					cpv1alpha1.DecisionKeyLabel:       "my-placement",
					cpv1alpha1.DecisionIndexLabel:     "0",
				}, []metav1.OwnerReference{ownerRefFor(newOCMPlacementDecision("ns1", "pd-update", "my-placement", "0", nil))},
					[]cpv1alpha1.ClusterDecision{
						{ClusterProfileRef: cpv1alpha1.ClusterProfileReference{Name: "cluster1", Namespace: "ns1"}, Reason: "score"},
						{ClusterProfileRef: cpv1alpha1.ClusterProfileReference{Name: "cluster2", Namespace: "ns1"}, Reason: "old"},
					}, SchedulerName),
			},
			expectedUpdates: 1,
			validateActions: func(t *testing.T, actions []clienttesting.Action) {
				updated := actions[0].(clienttesting.UpdateAction).GetObject().(*cpv1alpha1.PlacementDecision)
				if len(updated.Decisions) != 2 {
					t.Fatalf("expected 2 decisions, got %d", len(updated.Decisions))
				}
				if updated.Decisions[1].ClusterProfileRef.Name != "cluster3" {
					t.Errorf("expected cluster3, got %s", updated.Decisions[1].ClusterProfileRef.Name)
				}
				if len(updated.OwnerReferences) != 1 {
					t.Fatalf("expected 1 owner reference, got %d", len(updated.OwnerReferences))
				}
			},
		},
		{
			name: "no update when SIG PD is already correct",
			key:  "ns1/pd-noop",
			ocmPDs: []runtime.Object{
				newOCMPlacementDecision("ns1", "pd-noop", "my-placement", "0", []v1beta1.ClusterDecision{
					{ClusterName: "cluster1", Reason: "score"},
				}),
			},
			existingSIGPDs: []runtime.Object{
				newSIGPlacementDecisionWithOwnerRefs("ns1", "pd-noop", map[string]string{
					cpv1alpha1.LabelClusterManagerKey: SchedulerName,
					cpv1alpha1.PlacementKeyLabel:      "my-placement",
					cpv1alpha1.DecisionKeyLabel:       "my-placement",
					decisionGroupIndexLabel:           "0",
				}, []metav1.OwnerReference{ownerRefFor(newOCMPlacementDecision("ns1", "pd-noop", "my-placement", "0", nil))},
					[]cpv1alpha1.ClusterDecision{
						{ClusterProfileRef: cpv1alpha1.ClusterProfileReference{Name: "cluster1", Namespace: "ns1"}, Reason: "score"},
					}, SchedulerName),
			},
			expectedUpdates: 0,
			expectedCreates: 0,
		},
		{
			name:            "no-op when OCM PD not found and no SIG PD exists",
			key:             "ns1/pd-nonexistent",
			ocmPDs:          nil,
			existingSIGPDs:  nil,
			expectedDeletes: 0,
			expectedCreates: 0,
		},
		{
			name: "update SIG PD when existing has extra labels beyond desired",
			key:  "ns1/pd-extra-label",
			ocmPDs: []runtime.Object{
				newOCMPlacementDecision("ns1", "pd-extra-label", "", "", nil),
			},
			existingSIGPDs: []runtime.Object{
				newSIGPlacementDecisionWithOwnerRefs("ns1", "pd-extra-label", map[string]string{
					cpv1alpha1.LabelClusterManagerKey: SchedulerName,
					"extra-label":                     "value",
				}, []metav1.OwnerReference{ownerRefFor(newOCMPlacementDecision("ns1", "pd-extra-label", "", "", nil))},
					[]cpv1alpha1.ClusterDecision{}, SchedulerName),
			},
			expectedUpdates: 1,
			validateActions: func(t *testing.T, actions []clienttesting.Action) {
				updated := actions[0].(clienttesting.UpdateAction).GetObject().(*cpv1alpha1.PlacementDecision)
				if _, ok := updated.Labels["extra-label"]; ok {
					t.Errorf("expected extra-label to be dropped from updated labels")
				}
			},
		},
		{
			name: "update SIG PD when owner reference is missing",
			key:  "ns1/pd-missing-ownerref",
			ocmPDs: []runtime.Object{
				newOCMPlacementDecision("ns1", "pd-missing-ownerref", "", "", nil),
			},
			existingSIGPDs: []runtime.Object{
				newSIGPlacementDecision("ns1", "pd-missing-ownerref", map[string]string{
					cpv1alpha1.LabelClusterManagerKey: SchedulerName,
				}, []cpv1alpha1.ClusterDecision{}, SchedulerName),
			},
			expectedUpdates: 1,
			validateActions: func(t *testing.T, actions []clienttesting.Action) {
				updated := actions[0].(clienttesting.UpdateAction).GetObject().(*cpv1alpha1.PlacementDecision)
				if len(updated.OwnerReferences) != 1 {
					t.Fatalf("expected the update to backfill 1 owner reference, got %d", len(updated.OwnerReferences))
				}
			},
		},
		{
			name: "skip update of SIG PD not owned by us",
			key:  "ns1/pd-foreign-update",
			ocmPDs: []runtime.Object{
				newOCMPlacementDecision("ns1", "pd-foreign-update", "my-placement", "0", []v1beta1.ClusterDecision{
					{ClusterName: "cluster1"},
				}),
			},
			existingSIGPDs: []runtime.Object{
				newSIGPlacementDecision("ns1", "pd-foreign-update", map[string]string{
					cpv1alpha1.LabelClusterManagerKey: "other-scheduler",
				}, []cpv1alpha1.ClusterDecision{
					{ClusterProfileRef: cpv1alpha1.ClusterProfileReference{Name: "different-cluster"}},
				}, "other-scheduler"),
			},
			expectedUpdates: 0,
			expectedCreates: 0,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clusterObjects := c.ocmPDs
			clusterClient := clusterfake.NewSimpleClientset(clusterObjects...)
			clusterInformers := clusterinformers.NewSharedInformerFactory(clusterClient, 0)

			cpClient := cpfake.NewSimpleClientset(c.existingSIGPDs...)
			sigPDInformers := cpinformers.NewSharedInformerFactory(cpClient, 0)

			for _, pd := range c.ocmPDs {
				clusterInformers.Cluster().V1beta1().PlacementDecisions().Informer().GetStore().Add(pd)
			}
			for _, pd := range c.existingSIGPDs {
				sigPDInformers.Apis().V1alpha1().PlacementDecisions().Informer().GetStore().Add(pd)
			}

			ctrl := &sigPlacementDecisionController{
				ocmPDLister: clusterInformers.Cluster().V1beta1().PlacementDecisions().Lister(),
				sigPDClient: cpClient,
				sigPDLister: sigPDInformers.Apis().V1alpha1().PlacementDecisions().Lister(),
			}

			syncCtx := testingcommon.NewFakeSyncContext(t, c.key)
			err := ctrl.sync(context.TODO(), syncCtx, c.key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			actions := cpClient.Actions()

			var creates, updates, deletes int
			for _, action := range actions {
				switch action.GetVerb() {
				case "create":
					creates++
				case "update":
					updates++
				case "delete":
					deletes++
				}
			}

			if creates != c.expectedCreates {
				t.Errorf("expected %d creates, got %d", c.expectedCreates, creates)
			}
			if updates != c.expectedUpdates {
				t.Errorf("expected %d updates, got %d", c.expectedUpdates, updates)
			}
			if deletes != c.expectedDeletes {
				t.Errorf("expected %d deletes, got %d", c.expectedDeletes, deletes)
			}

			if c.validateActions != nil {
				mutatingActions := []clienttesting.Action{}
				for _, a := range actions {
					if a.GetVerb() != "get" && a.GetVerb() != "list" && a.GetVerb() != "watch" {
						mutatingActions = append(mutatingActions, a)
					}
				}
				c.validateActions(t, mutatingActions)
			}
		})
	}
}

func TestBuildSIGPlacementDecision(t *testing.T) {
	ocmPD := newOCMPlacementDecision("ns1", "my-placement-decision-3", "my-placement", "2", []v1beta1.ClusterDecision{
		{ClusterName: "cluster-a", Reason: "top-score"},
		{ClusterName: "cluster-b", Reason: ""},
	})
	ocmPD.Labels[v1beta1.DecisionGroupNameLabel] = "production"

	result := buildSIGPlacementDecision(ocmPD)

	if result.Name != "my-placement-decision-3" {
		t.Errorf("expected name my-placement-decision-3, got %s", result.Name)
	}
	if result.Namespace != "ns1" {
		t.Errorf("expected namespace ns1, got %s", result.Namespace)
	}
	if result.SchedulerName != SchedulerName {
		t.Errorf("expected scheduler %s, got %s", SchedulerName, result.SchedulerName)
	}
	if result.Labels[cpv1alpha1.LabelClusterManagerKey] != SchedulerName {
		t.Errorf("expected cluster-manager label")
	}
	if result.Labels[cpv1alpha1.PlacementKeyLabel] != "my-placement" {
		t.Errorf("expected placement-key label")
	}
	if result.Labels[cpv1alpha1.DecisionKeyLabel] != "my-placement" {
		t.Errorf("expected decision-key label")
	}
	if result.Labels[cpv1alpha1.DecisionIndexLabel] != "2" {
		t.Errorf("expected decision-index label '2', got %s", result.Labels[cpv1alpha1.DecisionIndexLabel])
	}
	if result.Labels[decisionGroupIndexLabel] != "2" {
		t.Errorf("expected decision-group-index label '2', got %s", result.Labels[decisionGroupIndexLabel])
	}
	if result.Labels[decisionGroupNameLabel] != "production" {
		t.Errorf("expected decision-group-name label 'production', got %s", result.Labels[decisionGroupNameLabel])
	}
	if len(result.Decisions) != 2 {
		t.Fatalf("expected 2 decisions, got %d", len(result.Decisions))
	}
	if result.Decisions[0].ClusterProfileRef.Name != "cluster-a" {
		t.Errorf("expected cluster-a, got %s", result.Decisions[0].ClusterProfileRef.Name)
	}
	if result.Decisions[0].ClusterProfileRef.Namespace != "ns1" {
		t.Errorf("expected namespace ns1, got %s", result.Decisions[0].ClusterProfileRef.Namespace)
	}
	if result.Decisions[0].Reason != "top-score" {
		t.Errorf("expected reason 'top-score', got %s", result.Decisions[0].Reason)
	}
	if result.Decisions[1].ClusterProfileRef.Name != "cluster-b" {
		t.Errorf("expected cluster-b, got %s", result.Decisions[1].ClusterProfileRef.Name)
	}
	if len(result.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(result.OwnerReferences))
	}
	ownerRef := result.OwnerReferences[0]
	if ownerRef.Name != ocmPD.Name {
		t.Errorf("expected owner reference name %s, got %s", ocmPD.Name, ownerRef.Name)
	}
	if ownerRef.Kind != "PlacementDecision" {
		t.Errorf("expected owner reference kind PlacementDecision, got %s", ownerRef.Kind)
	}
	if ownerRef.APIVersion != v1beta1.SchemeGroupVersion.String() {
		t.Errorf("expected owner reference apiVersion %s, got %s", v1beta1.SchemeGroupVersion.String(), ownerRef.APIVersion)
	}
	if ownerRef.Controller == nil || !*ownerRef.Controller {
		t.Errorf("expected owner reference to be a controller reference")
	}
	if ownerRef.BlockOwnerDeletion == nil || !*ownerRef.BlockOwnerDeletion {
		t.Errorf("expected owner reference to block owner deletion")
	}
}

func TestBuildSIGPlacementDecisionLabels(t *testing.T) {
	tests := []struct {
		name                  string
		placementDecisionName string
		placementName         string
		groupIndex            string
		groupName             string
		expectedDecisionIndex string
		expectedGroupIndex    string
		expectedGroupName     string
	}{
		{
			name:                  "first slice of the first group",
			placementDecisionName: "rollout-decision-1",
			placementName:         "rollout",
			groupIndex:            "0",
			groupName:             "canary",
			expectedDecisionIndex: "0",
			expectedGroupIndex:    "0",
			expectedGroupName:     "canary",
		},
		{
			name:                  "second slice shares the first group index",
			placementDecisionName: "rollout-decision-2",
			placementName:         "rollout",
			groupIndex:            "0",
			groupName:             "canary",
			expectedDecisionIndex: "1",
			expectedGroupIndex:    "0",
			expectedGroupName:     "canary",
		},
		{
			name:                  "third slice starts the next group",
			placementDecisionName: "rollout-decision-3",
			placementName:         "rollout",
			groupIndex:            "1",
			groupName:             "production",
			expectedDecisionIndex: "2",
			expectedGroupIndex:    "1",
			expectedGroupName:     "production",
		},
		{
			name:                  "manual name omits only the slice index",
			placementDecisionName: "manual-decision",
			placementName:         "rollout",
			groupIndex:            "2",
			groupName:             "manual",
			expectedGroupIndex:    "2",
			expectedGroupName:     "manual",
		},
		{
			name:                  "invalid group index omits group labels",
			placementDecisionName: "rollout-decision-4",
			placementName:         "rollout",
			groupIndex:            "-1",
			groupName:             "invalid",
			expectedDecisionIndex: "3",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ocmPD := newOCMPlacementDecision("rollout-ns", test.placementDecisionName,
				test.placementName, test.groupIndex, nil)
			ocmPD.Labels[v1beta1.DecisionGroupNameLabel] = test.groupName

			result := buildSIGPlacementDecision(ocmPD)

			if result.Labels[cpv1alpha1.DecisionIndexLabel] != test.expectedDecisionIndex {
				t.Errorf("expected decision-index %q, got %q",
					test.expectedDecisionIndex, result.Labels[cpv1alpha1.DecisionIndexLabel])
			}
			if result.Labels[decisionGroupIndexLabel] != test.expectedGroupIndex {
				t.Errorf("expected decision-group-index %q, got %q",
					test.expectedGroupIndex, result.Labels[decisionGroupIndexLabel])
			}
			if result.Labels[decisionGroupNameLabel] != test.expectedGroupName {
				t.Errorf("expected decision-group-name %q, got %q",
					test.expectedGroupName, result.Labels[decisionGroupNameLabel])
			}
		})
	}
}

func TestBuildSIGPlacementDecisionUsesSourceNamespaceForClusterProfile(t *testing.T) {
	for _, namespace := range []string{"development", "production"} {
		ocmPD := newOCMPlacementDecision(namespace, "rollout-decision-1", "rollout", "0",
			[]v1beta1.ClusterDecision{{ClusterName: "shared-cluster"}})

		result := buildSIGPlacementDecision(ocmPD)

		if result.Decisions[0].ClusterProfileRef.Namespace != namespace {
			t.Errorf("expected ClusterProfile shared-cluster in namespace %q, got %q",
				namespace, result.Decisions[0].ClusterProfileRef.Namespace)
		}
	}
}
