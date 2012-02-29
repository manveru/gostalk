package gostalker

import (
  "container/heap"
)

type delayedJobs []*Job

func newDelayedJobs() (jobs *delayedJobs) {
  jobs = &delayedJobs{}
  heap.Init(jobs)
  return
}

func (jobs delayedJobs) Len() int {
  return len(jobs)
}

func (jobs *delayedJobs) Less(a, b int) bool {
  return (*jobs)[a].delay.Before((*jobs)[b].delay)
}

func (jobs *delayedJobs) Pop() (job interface{}) {
  *jobs, job = (*jobs)[:jobs.Len()-1], (*jobs)[jobs.Len()-1]
  return
}

func (jobs *delayedJobs) Push(job interface{}) {
  *jobs = append(*jobs, job.(*Job))
}

func (jobs *delayedJobs) Swap(a, b int) {
  (*jobs)[a], (*jobs)[b] = (*jobs)[b], (*jobs)[a]
}
