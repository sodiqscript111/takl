package membership

import (
	"testing"
	"time"
)

func TestClusterJoinAndLeave(t *testing.T) {
	node1, err := NewCluster("node1", 7001, "127.0.0.1:8001", nil)
	if err != nil {
		t.Fatalf("node1: %v", err)
	}
	defer node1.Shutdown()

	node2, err := NewCluster("node2", 7002, "127.0.0.1:8002", []string{"127.0.0.1:7001"})
	if err != nil {
		t.Fatalf("node2: %v", err)
	}
	defer node2.Shutdown()

	time.Sleep(500 * time.Millisecond)

	m1 := node1.Members()
	if len(m1) != 1 || m1[0].NodeID != "node2" {
		t.Errorf("node1 expected 1 member (node2), got %v", m1)
	}

	m2 := node2.Members()
	if len(m2) != 1 || m2[0].NodeID != "node1" {
		t.Errorf("node2 expected 1 member (node1), got %v", m2)
	}
}
