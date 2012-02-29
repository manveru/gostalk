package gostalker

type delayedJobs []*Job

func (jobs delayedJobs) Len() int {
  return len(jobs)
}

func (jobs delayedJobs) Less(a, b int) bool {
  return jobs[a].priority > jobs[b].priority
}

func (jobs *delayedJobs) Pop() (job interface{}) {
  arr := *jobs
  size := len(arr)
  job = arr[size]
  *jobs = arr[:size-1]
  return
}

func (jobs *delayedJobs) Push(job interface{}) {
  *jobs = append(*jobs, job.(*Job))
}

func (jobs delayedJobs) Swap(a, b int) {
  jobs[a], jobs[b] = jobs[b], jobs[a]
}
