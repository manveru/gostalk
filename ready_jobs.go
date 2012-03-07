package gostalk

import (
	"code.google.com/p/go-priority-queue/prio"
)

type readyJobsItem job

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

func (jobs *readyJobs) getJob() (j *job) {
	j = (*job)(jobs.Pop().(*readyJobsItem))
	j.jobHolder = nil
	return
}

func (jobs *readyJobs) putJob(j *job) {
	j.jobHolder = jobs
	j.state = jobReadyState
	jobs.Push((*readyJobsItem)(j))
}

func (jobs *readyJobs) deleteJob(j *job) {
	jobs.Remove(j.index)
	j.jobHolder = nil
}

func (jobs *readyJobs) touchJob(j *job) {
}

func (jobs *readyJobs) buryJob(j *job) {
	jobs.deleteJob(j)
	j.tube.buried.putJob(j)
}

func (jobs *readyJobs) peekJob(request *jobPeekRequest) {
	request.success <- (*job)(jobs.Peek().(*readyJobsItem))
}
