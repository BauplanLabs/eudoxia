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

type Operator struct {
	ID                string `json:"id"`
	State             string `json:"state"`
	IsAssignableState bool   `json:"is_assignable_state"`
	ParentsComplete   bool   `json:"parents_complete"`
}

type Pipeline struct {
	PipelineID  string     `json:"pipeline_id"`
	Priority    string     `json:"priority"`
	ArrivalTick *int       `json:"arrival_tick"`
	IsComplete  bool       `json:"is_complete"`
	HasFailures bool       `json:"has_failures"`
	Operators   []Operator `json:"operators"`
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

type ExecutionResult struct {
	Ops         []string `json:"ops"`
	CPU         int      `json:"cpu"`
	RAM         float64  `json:"ram"`
	Priority    string   `json:"priority"`
	PoolID      int      `json:"pool_id"`
	ContainerID string   `json:"container_id"`
	Error       *string  `json:"error"`
}

type InitRequest struct {
	Params map[string]interface{} `json:"params"`
}

type ScheduleRequest struct {
	Tick                 int               `json:"tick"`
	SimTimeSeconds       float64           `json:"sim_time_seconds"`
	Results              []ExecutionResult `json:"results"`
	NewPipelines         []Pipeline        `json:"new_pipelines"`
	OutstandingPipelines []Pipeline        `json:"outstanding_pipelines"`
	Pools                []Pool            `json:"pools"`
}

type Suspension struct {
	ContainerID string `json:"container_id"`
	PoolID      int    `json:"pool_id"`
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

type ScheduleResponse struct {
	Suspensions []Suspension `json:"suspensions"`
	Assignments []Assignment `json:"assignments"`
}

// =============================================================================
// Scheduler State
// =============================================================================

type NaiveScheduler struct {
	waitingQueue []string // pipeline IDs in FIFO order
}

var scheduler = &NaiveScheduler{
	waitingQueue: []string{},
}

// =============================================================================
// Handlers
// =============================================================================

func initHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req InitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Reset state
	scheduler.waitingQueue = []string{}

	log.Printf("Init")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func scheduleHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := schedule(&req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// =============================================================================
// Scheduling Logic: FIFO queue, one operator per pool, all pool resources
// =============================================================================

func schedule(req *ScheduleRequest) *ScheduleResponse {
	// Add new pipelines to waiting queue
	for _, p := range req.NewPipelines {
		scheduler.waitingQueue = append(scheduler.waitingQueue, p.PipelineID)
	}

	// Build lookup maps
	pipelineByID := make(map[string]*Pipeline)
	for i := range req.OutstandingPipelines {
		p := &req.OutstandingPipelines[i]
		pipelineByID[p.PipelineID] = p
	}
	// Also include new pipelines in lookup
	for i := range req.NewPipelines {
		p := &req.NewPipelines[i]
		pipelineByID[p.PipelineID] = p
	}

	assignments := []Assignment{}
	var requeuePipelines []string

	// Try to assign work to each pool
	for _, pool := range req.Pools {
		if pool.AvailCPU <= 0 || pool.AvailRAMGB <= 0 {
			continue
		}

		// Find a pipeline with ops we can assign
		for len(scheduler.waitingQueue) > 0 {
			pipelineID := scheduler.waitingQueue[0]
			scheduler.waitingQueue = scheduler.waitingQueue[1:]

			pipeline, ok := pipelineByID[pipelineID]
			if !ok {
				continue // Pipeline not found (shouldn't happen)
			}

			// Check if pipeline is complete or has failures
			if pipeline.IsComplete || pipeline.HasFailures {
				continue // Don't retry, permanently remove
			}

			// Might have more work later
			requeuePipelines = append(requeuePipelines, pipelineID)

			// Get first assignable operator with parents complete
			opID := getFirstAssignableOp(pipeline)
			if opID == "" {
				continue // Might be ready later
			}

			// Assign all resources of this pool to this pipeline
			assignment := Assignment{
				OperatorIDs: []string{opID},
				CPU:         pool.AvailCPU,
				RAMGB:       pool.AvailRAMGB,
				PoolID:      pool.PoolID,
				Priority:    pipeline.Priority,
				IsResume:    false,
				ForceRun:    false,
			}
			assignments = append(assignments, assignment)
			break // Move to next pool
		}
	}

	// Requeue pipelines that might have more work
	scheduler.waitingQueue = append(scheduler.waitingQueue, requeuePipelines...)

	log.Printf("Tick %d: %d assignments, queue size: %d",
		req.Tick, len(assignments), len(scheduler.waitingQueue))

	return &ScheduleResponse{
		Suspensions: []Suspension{},
		Assignments: assignments,
	}
}

func getFirstAssignableOp(p *Pipeline) string {
	for _, op := range p.Operators {
		if op.IsAssignableState && op.ParentsComplete {
			return op.ID
		}
	}
	return ""
}

// =============================================================================
// Main
// =============================================================================

func main() {
	port := flag.Int("port", 8080, "Port to listen on")
	flag.Parse()

	http.HandleFunc("/init", initHandler)
	http.HandleFunc("/schedule", scheduleHandler)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Naive scheduler listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
