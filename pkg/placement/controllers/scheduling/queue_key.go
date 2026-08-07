package scheduling

import (
	"fmt"
	"strings"
)

type schedulingQueueKeyType string

const (
	placementQueueKeyType         schedulingQueueKeyType = "placement"
	clusterSetQueueKeyType        schedulingQueueKeyType = "cluster-set"
	clusterSetBindingQueueKeyType schedulingQueueKeyType = "cluster-set-binding"
	placementScoreQueueKeyType    schedulingQueueKeyType = "placement-score"
)

type schedulingQueueKey struct {
	keyType   schedulingQueueKeyType
	namespace string
	name      string
}

func newPlacementQueueKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", placementQueueKeyType, namespace, name)
}

func newClusterSetQueueKey(name string) string {
	return fmt.Sprintf("%s/%s", clusterSetQueueKeyType, name)
}

func newClusterSetBindingQueueKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", clusterSetBindingQueueKeyType, namespace, name)
}

func newPlacementScoreQueueKey(clusterName, name string) string {
	return fmt.Sprintf("%s/%s/%s", placementScoreQueueKeyType, clusterName, name)
}

func parseSchedulingQueueKey(key string) (schedulingQueueKey, error) {
	parts := strings.Split(key, "/")
	if len(parts) < 2 {
		return schedulingQueueKey{}, fmt.Errorf("invalid scheduling queue key %q", key)
	}

	queueKey := schedulingQueueKey{keyType: schedulingQueueKeyType(parts[0])}
	switch queueKey.keyType {
	case clusterSetQueueKeyType:
		if len(parts) != 2 || parts[1] == "" {
			return schedulingQueueKey{}, fmt.Errorf("invalid cluster set queue key %q", key)
		}
		queueKey.name = parts[1]
	case placementQueueKeyType, clusterSetBindingQueueKeyType, placementScoreQueueKeyType:
		if len(parts) != 3 || parts[1] == "" || parts[2] == "" {
			return schedulingQueueKey{}, fmt.Errorf("invalid %s queue key %q", queueKey.keyType, key)
		}
		queueKey.namespace = parts[1]
		queueKey.name = parts[2]
	default:
		return schedulingQueueKey{}, fmt.Errorf("unsupported scheduling queue key type %q", parts[0])
	}

	return queueKey, nil
}
