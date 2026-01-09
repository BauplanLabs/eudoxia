// Naive scheduler implementation for the Eudoxia REST scheduler interface.
//
// Usage:
//   cd go && go run ./naive/ [-port 8080]
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

	"github.com/BauplanLabs/eudoxia/go/eudoxia"
)

type NaiveScheduler struct{}

func (s *NaiveScheduler) handleInit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req eudoxia.InitRequest
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

	var req eudoxia.ScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := s.schedule(&req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// schedule is stateless: one operator per pool, all pool resources
func (s *NaiveScheduler) schedule(req *eudoxia.ScheduleRequest) *eudoxia.ScheduleResponse {
	// Combine all pipelines (new first, then other)
	allPipelines := make([]*eudoxia.Pipeline, 0, len(req.NewPipelines)+len(req.OtherPipelines))
	for i := range req.NewPipelines {
		allPipelines = append(allPipelines, &req.NewPipelines[i])
	}
	for i := range req.OtherPipelines {
		allPipelines = append(allPipelines, &req.OtherPipelines[i])
	}

	// Track which operators we've already assigned this tick
	assigned := make(map[string]bool)
	assignments := []eudoxia.Assignment{}

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

				assignments = append(assignments, eudoxia.Assignment{
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

	return &eudoxia.ScheduleResponse{
		Suspensions: []eudoxia.Suspension{},
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
