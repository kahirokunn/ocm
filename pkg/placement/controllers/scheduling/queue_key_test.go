package scheduling

import "testing"

func TestParseSchedulingQueueKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected schedulingQueueKey
		wantErr  bool
	}{
		{
			name: "placement",
			key:  newPlacementQueueKey("namespace1", "placement1"),
			expected: schedulingQueueKey{
				keyType:   placementQueueKeyType,
				namespace: "namespace1",
				name:      "placement1",
			},
		},
		{
			name: "cluster set",
			key:  newClusterSetQueueKey("clusterset1"),
			expected: schedulingQueueKey{
				keyType: clusterSetQueueKeyType,
				name:    "clusterset1",
			},
		},
		{
			name: "cluster set binding",
			key:  newClusterSetBindingQueueKey("namespace1", "clusterset1"),
			expected: schedulingQueueKey{
				keyType:   clusterSetBindingQueueKeyType,
				namespace: "namespace1",
				name:      "clusterset1",
			},
		},
		{
			name: "placement score",
			key:  newPlacementScoreQueueKey("cluster1", "score1"),
			expected: schedulingQueueKey{
				keyType:   placementScoreQueueKeyType,
				namespace: "cluster1",
				name:      "score1",
			},
		},
		{name: "legacy placement key", key: "namespace1/placement1", wantErr: true},
		{name: "unknown type", key: "cluster/cluster1", wantErr: true},
		{name: "empty cluster set name", key: "cluster-set/", wantErr: true},
		{name: "missing placement name", key: "placement/namespace1", wantErr: true},
		{name: "extra segment", key: "placement/namespace1/placement1/extra", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := parseSchedulingQueueKey(test.key)
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected an error for key %q", test.key)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if actual != test.expected {
				t.Errorf("expected %#v, got %#v", test.expected, actual)
			}
		})
	}
}
