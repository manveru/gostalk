package gostalker

type reservedJobs []*Job

func (jobs reservedJobs) Len() int {
  return len(jobs)
}

func (jobs reservedJobs) Less(a, b int) bool {
  return jobs[a].priority > jobs[b].priority
}

func (jobs *reservedJobs) Pop() (job interface{}) {
  arr := *jobs
  size := len(arr)
  job = arr[size]
  *jobs = arr[:size-1]
  return
}

func (jobs *reservedJobs) Push(job interface{}) {
  *jobs = append(*jobs, job.(*Job))
}

func (jobs reservedJobs) Swap(a, b int) {
  jobs[a], jobs[b] = jobs[b], jobs[a]
}
