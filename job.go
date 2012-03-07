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
	deleteJob(*job)
	touchJob(*job)
	buryJob(*job)
}

type jobId uint64
type job struct {
	body                                                                  []byte
	client                                                                *client
	id                                                                    jobId
	jobHolder                                                             jobHolder
	priority                                                              uint32
	state                                                                 string
	tube                                                                  *tube
	timer                                                                 *time.Timer
	timeToReserve                                                         time.Duration
	createdAt, delayEndsAt, reserveEndsAt                                 time.Time
	index, reserveCount, releaseCount, timeoutCount, buryCount, kickCount int
}

func newJob(id jobId, priority uint32, delay int64, ttr int64, body []byte) *job {
	j := &job{
		id:            id,
		priority:      priority,
		createdAt:     time.Now(),
		timeToReserve: time.Duration(ttr * int64(time.Second)),
		body:          body,
	}

	if delay > 0 {
		j.state = jobWillHaveDelayedState
		j.delayEndsAt = time.Now().Add(time.Duration(delay) * time.Second)
	}

	return j
}

// the number of seconds left until the server puts this job into the ready
// queue. This number is only meaningful if the job is reserved or delayed. If
// the job is reserved and this amount of time elapses before its state
// changes, it is considered to have timed out.
func (job job) timeLeft() (left time.Duration) {
	switch job.state {
	case jobReservedState:
		left = job.reserveEndsAt.Sub(time.Now())
	case jobDelayedState:
		left = job.delayEndsAt.Sub(time.Now())
	}
	return
}

func (job *job) deleteFrom(server *server) {
	delete(server.jobs, job.id)
	job.tube.jobDelete <- job
}

func (job *job) bury() {
	job.tube.jobBury <- job
}

func (job *job) touch() {
	job.tube.jobTouch <- job
}

func (job *job) isUrgent() bool {
	return job.priority > 1024
}
