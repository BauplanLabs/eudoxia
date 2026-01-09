// Naive scheduler implementation in Go for the Eudoxia REST scheduler interface.
//
// Usage:
//   go run ./go/naive.go [-port 8080]
//
// Then run the simulator with:
//   eudoxia run mysim.toml
//
// Where mysim.toml contains:
//   scheduler_algo = "rest"
//   rest_scheduler_addr = "localhost:8080"

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
)

// =============================================================================
// JSON Schema Types (matching the REST interface)
// =============================================================================

type InitRequest struct {
	Params map[string]interface{} `json:"params"`
}

type ScheduleRequest struct {
	Tick           int               `json:"tick"`
	SimTimeSeconds float64           `json:"sim_time_seconds"`
	Results        []ExecutionResult `json:"results"`
	NewPipelines   []Pipeline        `json:"new_pipelines"`
	OtherPipelines []Pipeline        `json:"other_pipelines"`
	Pools          []Pool            `json:"pools"`
}

type ScheduleResponse struct {
	Suspensions []Suspension `json:"suspensions"`
	Assignments []Assignment `json:"assignments"`
}

type Pipeline struct {
	PipelineID  string     `json:"pipeline_id"`
	Priority    string     `json:"priority"`
	ArrivalTick *int       `json:"arrival_tick"`
	IsComplete  bool       `json:"is_complete"`
	HasFailures bool       `json:"has_failures"`
	Operators   []Operator `json:"operators"`
}

type Operator struct {
	ID                string `json:"id"`
	State             string `json:"state"`
	IsAssignableState bool   `json:"is_assignable_state"`
	ParentsComplete   bool   `json:"parents_complete"`
}

type Pool struct {
	PoolID               int         `json:"pool_id"`
	MaxCPU               int         `json:"max_cpu"`
	MaxRAMGB             float64     `json:"max_ram_gb"`
	AvailCPU             int         `json:"avail_cpu"`
	AvailRAMGB           float64     `json:"avail_ram_gb"`
	ConsumedRAMGB        float64     `json:"consumed_ram_gb"`
	ActiveContainers     []Container `json:"active_containers"`
	SuspendingContainers []Container `json:"suspending_containers"`
	SuspendedContainers  []Container `json:"suspended_containers"`
}

type Container struct {
	ContainerID     string   `json:"container_id"`
	PipelineID      string   `json:"pipeline_id"`
	OperatorIDs     []string `json:"operator_ids"`
	CPU             int      `json:"cpu"`
	RAMGB           float64  `json:"ram_gb"`
	CurrentMemoryGB float64  `json:"current_memory_gb"`
	Priority        string   `json:"priority"`
}

type Assignment struct {
	OperatorIDs []string `json:"operator_ids"`
	CPU         int      `json:"cpu"`
	RAMGB       float64  `json:"ram_gb"`
	PoolID      int      `json:"pool_id"`
	Priority    string   `json:"priority"`
	IsResume    bool     `json:"is_resume"`
	ForceRun    bool     `json:"force_run"`
}

type Suspension struct {
	ContainerID string `json:"container_id"`
	PoolID      int    `json:"pool_id"`
}

type ExecutionResult struct {
	Ops         []string `json:"ops"`
	CPU         int      `json:"cpu"`
	RAM         float64  `json:"ram"`
	Priority    string   `json:"priority"`
	PoolID      int      `json:"pool_id"`
	ContainerID string   `json:"container_id"`
	Error       *string  `json:"error"`
}


// =============================================================================
// Scheduler
// =============================================================================

type NaiveScheduler struct{}

func (s *NaiveScheduler) handleInit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req InitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Init")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *NaiveScheduler) handleSchedule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := s.schedule(&req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// schedule is stateless: one operator per pool, all pool resources
func (s *NaiveScheduler) schedule(req *ScheduleRequest) *ScheduleResponse {
	// Combine all pipelines (new first, then other)
	allPipelines := make([]*Pipeline, 0, len(req.NewPipelines)+len(req.OtherPipelines))
	for i := range req.NewPipelines {
		allPipelines = append(allPipelines, &req.NewPipelines[i])
	}
	for i := range req.OtherPipelines {
		allPipelines = append(allPipelines, &req.OtherPipelines[i])
	}

	// Track which operators we've already assigned this tick
	assigned := make(map[string]bool)
	assignments := []Assignment{}

	// Try to assign work to each pool
	for _, pool := range req.Pools {
		if pool.AvailCPU <= 0 || pool.AvailRAMGB <= 0 {
			continue
		}

		// Find an assignable operator
		for _, pipeline := range allPipelines {
			if pipeline.IsComplete || pipeline.HasFailures {
				continue
			}

			found := false
			for _, op := range pipeline.Operators {
				if !op.IsAssignableState || !op.ParentsComplete || assigned[op.ID] {
					continue
				}

				assignments = append(assignments, Assignment{
					OperatorIDs: []string{op.ID},
					CPU:         pool.AvailCPU,
					RAMGB:       pool.AvailRAMGB,
					PoolID:      pool.PoolID,
					Priority:    pipeline.Priority,
					IsResume:    false,
					ForceRun:    false,
				})
				assigned[op.ID] = true
				found = true
				break
			}
			if found {
				break
			}
		}
	}

	log.Printf("Tick %d: %d assignments", req.Tick, len(assignments))

	return &ScheduleResponse{
		Suspensions: []Suspension{},
		Assignments: assignments,
	}
}

func main() {
	port := flag.Int("port", 8080, "Port to listen on")
	flag.Parse()

	scheduler := &NaiveScheduler{}
	http.HandleFunc("/init", scheduler.handleInit)
	http.HandleFunc("/schedule", scheduler.handleSchedule)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Naive scheduler listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
