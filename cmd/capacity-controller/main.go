package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type metrics struct {
	Items []struct {
		Usage map[string]string `json:"usage"`
	} `json:"items"`
}
type run struct {
	RunID string `json:"run_id"`
	State string `json:"state"`
}

func main() {
	base := env("EXPERIMENT_URL", "http://load-test-service:8080")
	current := envInt("CAPACITY_START_RPS", 100)
	max := envInt("CAPACITY_MAX_RPS", 20000)
	limit := envInt("CAPACITY_CPU_LIMIT", 90)
	if current > max {
		current = max
	}
	read := max * 4 / 5
	write := max - read
	id := start(base, read, write)
	fmt.Printf("started 10s ramp run=%s read_rps=%d write_rps=%d total_rps=%d\n", id, read, write, max)
	for {
		state := status(base, id)
		cpu := nodeCPU()
		fmt.Printf("run=%s state=%s cpu=%d%%\n", id, state, cpu)
		if cpu >= limit {
			stop(base, id)
			fmt.Printf("capacity reached cpu=%d%% total_rps=%d run_id=%s\n", cpu, max, id)
			return
		}
		if state == "PASS" || state == "FAILED" {
			break
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Printf("capacity limit not reached during 10s ramp; max_rps=%d\n", max)
}

func start(base string, read, write int) string {
	body, _ := json.Marshal(map[string]any{"load_mode": "manual", "workload": "resilience", "profile": "capacity-ramp", "duration": "10s", "read_rps": read, "write_rps": write})
	var out run
	do(http.MethodPost, base+"/api/runs", body, &out)
	return out.RunID
}
func status(base, id string) string {
	var out run
	do(http.MethodGet, base+"/runs/"+id, nil, &out)
	return out.State
}
func stop(base, id string) {
	req, _ := http.NewRequest(http.MethodDelete, base+"/runs/"+id, nil)
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}
func nodeCPU() int {
	token, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	ca, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	pool := new(tls.Config)
	_ = ca
	_ = pool
	req, _ := http.NewRequest(http.MethodGet, "https://kubernetes.default.svc:443/apis/metrics.k8s.io/v1beta1/nodes", nil)
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} // metrics endpoint is the in-cluster API server
	resp, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		fmt.Printf("metrics request error: %v\n", err)
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("metrics request status=%s body=%s\n", resp.Status, strings.TrimSpace(string(body)))
		return 0
	}
	var m metrics
	if json.NewDecoder(resp.Body).Decode(&m) != nil {
		fmt.Println("metrics response decode failed")
		return 0
	}
	totalNano := 0.0
	for _, item := range m.Items {
		v := parseNanoCPU(item.Usage["cpu"])
		totalNano += v
	}
	cores := envInt("CAPACITY_NODE_CPU_CORES", 16)
	return int(totalNano / (1e9 * float64(cores)) * 100)
}
func parseNanoCPU(v string) float64 {
	v = strings.TrimSpace(v)
	if strings.HasSuffix(v, "n") {
		n, _ := strconv.ParseFloat(strings.TrimSuffix(v, "n"), 64)
		return n
	}
	if strings.HasSuffix(v, "m") {
		n, _ := strconv.ParseFloat(strings.TrimSuffix(v, "m"), 64)
		return n * 1e6
	}
	n, _ := strconv.ParseFloat(v, 64)
	return n * 1e9
}
func do(method, url string, body []byte, out any) {
	req, _ := http.NewRequestWithContext(context.Background(), method, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("%s: %s", resp.Status, string(b)))
	}
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	n, e := strconv.Atoi(env(k, ""))
	if e != nil || n <= 0 {
		return d
	}
	return n
}
