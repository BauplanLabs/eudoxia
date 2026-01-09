// Package eudoxia provides types for the Eudoxia REST scheduler interface.
//
// External schedulers can import this package to get the request/response
// types for implementing the /init and /schedule endpoints.
package eudoxia

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
