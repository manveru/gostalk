package gostalk

import (
	"code.google.com/p/go-priority-queue/prio"
	"time"
)

type reservedJobsItem job

func (i *reservedJobsItem) Less(j prio.Interface) bool {
	return i.reserveEndsAt.Before(j.(*reservedJobsItem).reserveEndsAt)
}

func (i *reservedJobsItem) Index(n int) {
	i.index = n
}

type reservedJobs struct {
	prio.Queue
}

func newReservedJobs() (jobs *reservedJobs) {
	return &reservedJobs{}
}

func (jobs *reservedJobs) getJob() (j *job) {
	j = (*job)(jobs.Pop().(*reservedJobsItem))
	j.jobHolder = nil
	return
}

func (jobs *reservedJobs) putJob(j *job) {
	j.jobHolder = jobs
	j.state = jobReservedState
	jobs.Push((*reservedJobsItem)(j))
}

func (jobs *reservedJobs) deleteJob(j *job) {
	jobs.Remove(j.index)
	j.jobHolder = nil
}

func (jobs *reservedJobs) touchJob(j *job) {
	jobs.Remove(j.index)
	j.reserveEndsAt = time.Now().Add(j.timeToReserve)
	jobs.Push((*reservedJobsItem)(j))
}

func (jobs *reservedJobs) buryJob(j *job) {
	jobs.deleteJob(j)
	j.tube.buried.putJob(j)
}

func (jobs *reservedJobs) peekJob(request *jobPeekRequest) {
	request.success <- (*job)(jobs.Peek().(*reservedJobsItem))
}
