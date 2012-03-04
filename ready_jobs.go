package gostalk

import (
  "code.google.com/p/go-priority-queue/prio"
)

type readyJobsItem Job

func (i *readyJobsItem) Less(j prio.Interface) bool {
  return i.priority < j.(*readyJobsItem).priority
}

func (i *readyJobsItem) Index(n int) {
  i.index = n
}

type readyJobs struct {
  prio.Queue
}

func newReadyJobs() (jobs *readyJobs) {
  return &readyJobs{}
}

func (jobs *readyJobs) getJob() (job *Job) {
  job = (*Job)(jobs.Pop().(*readyJobsItem))
  job.jobHolder = nil
  return
}

func (jobs *readyJobs) putJob(job *Job) {
  job.jobHolder = jobs
  job.state = jobReadyState
  jobs.Push((*readyJobsItem)(job))
}

func (jobs *readyJobs) deleteJob(job *Job) {
  jobs.Remove(job.index)
  job.jobHolder = nil
}

func (jobs *readyJobs) touchJob(job *Job) {
}

func (jobs *readyJobs) buryJob(job *Job) {
  jobs.deleteJob(job)
  job.tube.buried.putJob(job)
}
