package gostalker

import (
  "container/heap"
)

type reservedJobs []*Job

func newReservedJobs() (jobs *reservedJobs) {
  jobs = &reservedJobs{}
  heap.Init(jobs)
  return
}

func (jobs reservedJobs) Len() int {
  return len(jobs)
}

func (jobs *reservedJobs) Less(a, b int) bool {
  return (*jobs)[a].delay.Before((*jobs)[b].delay)
}

func (jobs *reservedJobs) Pop() (job interface{}) {
  *jobs, job = (*jobs)[:jobs.Len()-1], (*jobs)[jobs.Len()-1]
  return
}

func (jobs *reservedJobs) Push(job interface{}) {
  *jobs = append(*jobs, job.(*Job))
}

func (jobs *reservedJobs) Swap(a, b int) {
  (*jobs)[a], (*jobs)[b] = (*jobs)[b], (*jobs)[a]
}

func (jobs *reservedJobs) getJob() *Job {
  return heap.Pop(jobs).(*Job)
}

func (jobs *reservedJobs) putJob(job *Job) {
  heap.Push(jobs, job)
}
