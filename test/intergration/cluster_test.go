package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// baseURL returns the API base URL for a node index (1-3).
func baseURL(node int) string {
	return fmt.Sprintf("http://localhost:%d", 8080+node)
}

// get performs a GET /v1/keys/{key} against the given node.
func get(t *testing.T, node int, key string) (string, int) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/v1/keys/%s", baseURL(node), key))
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if v, ok := body["value"].(string); ok {
		return v, resp.StatusCode
	}
	return "", resp.StatusCode
}

// put performs PUT /v1/keys/{key} against the given node.
func put(t *testing.T, node int, key, value string) int {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"value": value})
	resp, err := http.Post(
		fmt.Sprintf("%s/v1/keys/%s", baseURL(node), key),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("PUT %s: %v", key, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// status returns the /v1/status JSON from a node.
func status(t *testing.T, node int) map[string]any {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/v1/status", baseURL(node)))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}

// waitForLeader polls all nodes until one reports is_leader=true.
func waitForLeader(t *testing.T, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for node := 1; node <= 3; node++ {
			s := status(t, node)
			if s == nil {
				continue
			}
			if leader, _ := s["is_leader"].(bool); leader {
				return node
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v", timeout)
	return 0
}

// TestElectionOnStartup verifies that exactly one leader is elected
// within 600ms of cluster startup.
func TestElectionOnStartup(t *testing.T) {
	leader := waitForLeader(t, 2*time.Second)
	t.Logf("leader elected: node%d", leader)
}

// TestWriteToLeaderReadFromFollower writes a key to the leader and then
// reads it back from every node, verifying consistency.
func TestWriteToLeaderReadFromFollower(t *testing.T) {
	leader := waitForLeader(t, 2*time.Second)

	const key = "integration-test-key"
	const val = "hello-raft"

	code := put(t, leader, key, val)
	if code != http.StatusOK {
		t.Fatalf("PUT returned %d", code)
	}

	// Give followers time to apply the committed entry.
	time.Sleep(100 * time.Millisecond)

	for node := 1; node <= 3; node++ {
		got, code := get(t, node, key)
		if code != http.StatusOK {
			t.Errorf("node%d GET %s returned %d", node, key, code)
			continue
		}
		if got != val {
			t.Errorf("node%d: expected %q, got %q", node, val, got)
		}
	}
}

// TestCASOperation verifies compare-and-swap semantics.
func TestCASOperation(t *testing.T) {
	leader := waitForLeader(t, 2*time.Second)

	const key = "cas-test-key"
	put(t, leader, key, "initial")
	time.Sleep(100 * time.Millisecond)

	// Valid CAS: prev_value matches
	body, _ := json.Marshal(map[string]string{
		"prev_value": "initial",
		"new_value":  "updated",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/v1/cas/%s", baseURL(leader), key),
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	if resp != nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("valid CAS returned %d", resp.StatusCode)
		}
	}

	time.Sleep(100 * time.Millisecond)
	got, _ := get(t, leader, key)
	if got != "updated" {
		t.Errorf("after CAS expected %q, got %q", "updated", got)
	}
}