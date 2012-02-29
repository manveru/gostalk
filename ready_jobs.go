package gostalker

import (
  "container/heap"
  "fmt"
)

type readyJobs []*Job

func newReadyJobs() (jobs *readyJobs) {
  jobs = &readyJobs{}
  heap.Init(jobs)
  return
}

func (jobs readyJobs) Len() int {
  return len(jobs)
}

func (jobs *readyJobs) Less(a, b int) bool {
  return (*jobs)[a].priority > (*jobs)[b].priority
}

func (jobs *readyJobs) Pop() (job interface{}) {
  *jobs, job = (*jobs)[:jobs.Len()-1], (*jobs)[jobs.Len()-1]
  return
}

func (jobs *readyJobs) Push(job interface{}) {
  *jobs = append(*jobs, job.(*Job))
}

func (jobs *readyJobs) Swap(a, b int) {
  fmt.Println("readyJobs.Swap(", a, ",", b, ")")
  (*jobs)[a], (*jobs)[b] = (*jobs)[b], (*jobs)[a]
}
