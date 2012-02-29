package gostalker

type readyJobs []*Job

func (jobs readyJobs) Len() int {
  return len(jobs)
}

func (jobs readyJobs) Less(a, b int) bool {
  return jobs[a].priority > jobs[b].priority
}

func (jobs *readyJobs) Pop() (job interface{}) {
  arr := *jobs
  size := len(arr)
  job = arr[size]
  *jobs = arr[:size-1]
  return
}

func (jobs *readyJobs) Push(job interface{}) {
  *jobs = append(*jobs, job.(*Job))
}

func (jobs readyJobs) Swap(a, b int) {
  jobs[a], jobs[b] = jobs[b], jobs[a]
}
