package gostalk

import (
	"time"
)

const (
	jobReadyState           = "ready"
	jobDelayedState         = "delayed"
	jobReservedState        = "reserved"
	jobBuriedState          = "buried"
	jobWillHaveDelayedState = "job will have delayed state"
)

type jobHolder interface {
	deleteJob(*Job)
	touchJob(*Job)
	buryJob(*Job)
}

type JobId uint64
type Job struct {
	body                                                                  []byte
	client                                                                *Client
	id                                                                    JobId
	jobHolder                                                             jobHolder
	priority                                                              uint32
	state                                                                 string
	tube                                                                  *Tube
	timer                                                                 *time.Timer
	timeToReserve                                                         time.Duration
	createdAt, delayEndsAt, reserveEndsAt                                 time.Time
	index, reserveCount, releaseCount, timeoutCount, buryCount, kickCount int
}

func newJob(id JobId, priority uint32, delay int64, ttr int64, body []byte) (job *Job) {
	job = &Job{
		id:            id,
		priority:      priority,
		createdAt:     time.Now(),
		timeToReserve: time.Duration(ttr * int64(time.Second)),
		body:          body,
	}

	if delay > 0 {
		job.state = jobWillHaveDelayedState
		job.delayEndsAt = time.Now().Add(time.Duration(delay) * time.Second)
	}

	return job
}

// the number of seconds left until the server puts this job into the ready
// queue. This number is only meaningful if the job is reserved or delayed. If
// the job is reserved and this amount of time elapses before its state
// changes, it is considered to have timed out.
func (job Job) timeLeft() (left time.Duration) {
	switch job.state {
	case jobReservedState:
		left = job.reserveEndsAt.Sub(time.Now())
	case jobDelayedState:
		left = job.delayEndsAt.Sub(time.Now())
	}
	return
}

func (job *Job) deleteFrom(server *Server) {
	delete(server.jobs, job.id)
	job.tube.jobDelete <- job
}

func (job *Job) bury() {
	job.tube.jobBury <- job
}

func (job *Job) touch() {
	job.tube.jobTouch <- job
}

func (job *Job) isUrgent() bool {
	return job.priority > 1024
}
