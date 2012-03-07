package gostalk

import (
	"time"
)

type jobReserveRequest struct {
	client  *client
	success chan *job
	cancel  chan bool
}

type jobKickRequest struct {
	bound   int
	success chan int
}

type jobPeekRequest struct {
	state   string
	success chan *job
}

type tube struct {
	name     string
	ready    *readyJobs
	reserved *reservedJobs
	buried   *buriedJobs
	delayed  *delayedJobs

	jobDemand chan *jobReserveRequest
	jobSupply chan *job
	jobDelete chan *job
	jobTouch  chan *job
	jobBury   chan *job
	jobKick   chan *jobKickRequest
	jobPeek   chan *jobPeekRequest
	tubePause chan time.Duration

	paused         bool
	pauseStartedAt time.Time
	pauseEndsAt    time.Time

	stats *tubeStats
}

func newTube(name string) *tube {
	t := &tube{
		name:      name,
		paused:    false,
		ready:     newReadyJobs(),
		reserved:  newReservedJobs(),
		buried:    newBuriedJobs(),
		delayed:   newDelayedJobs(),
		jobDemand: make(chan *jobReserveRequest),
		jobSupply: make(chan *job),
		jobDelete: make(chan *job),
		jobTouch:  make(chan *job),
		jobBury:   make(chan *job),
		jobKick:   make(chan *jobKickRequest),
		stats:     &tubeStats{Name: name},
	}

	go t.handleDemand()

	return t
}

func (tube *tube) handleDemand() {
	for {
		if tube.ready.Len() > 0 {
			select {
			case duration := <-tube.tubePause:
				tube.pause(duration)
			case job := <-tube.jobBury:
				tube.bury(job)
			case job := <-tube.jobDelete:
				tube.delete(job)
			case job := <-tube.jobSupply:
				tube.put(job)
			case job := <-tube.jobTouch:
				tube.touch(job)
			case request := <-tube.jobKick:
				request.success <- tube.kick(request.bound)
			case request := <-tube.jobPeek:
				tube.peek(request)
			case request := <-tube.jobDemand:
				select {
				case request.success <- tube.reserve(request.client):
				case <-request.cancel:
					request.cancel <- true // propagate to the other tubes
				}
			}
		} else {
			select {
			case duration := <-tube.tubePause:
				tube.pause(duration)
			case job := <-tube.jobBury:
				tube.bury(job)
			case job := <-tube.jobDelete:
				tube.delete(job)
			case job := <-tube.jobTouch:
				tube.touch(job)
			case request := <-tube.jobPeek:
				tube.peek(request)
			case request := <-tube.jobKick:
				request.success <- tube.kick(request.bound)
			case job := <-tube.jobSupply:
				tube.put(job)
			}
		}
	}
}

func (tube *tube) reserve(client *client) (job *job) {
	job = tube.ready.getJob()

	tube.reserved.putJob(job)

	job.client = client
	job.state = jobReservedState
	job.reserveCount += 1
	job.reserveEndsAt = time.Now().Add(job.timeToReserve)

	return
}

func (tube *tube) put(job *job) {
	job.tube = tube
	if job.isUrgent() {
		tube.stats.CurrentUrgentJobs += 1
	}

	if job.state == jobWillHaveDelayedState {
		tube.delayed.putJob(job, func() {
			job.state = ""
			tube.jobSupply <- job
		})
	} else {
		tube.ready.putJob(job)
	}
}

func (tube *tube) delete(job *job) {
	if job.isUrgent() {
		tube.stats.CurrentUrgentJobs -= 1
	}
	job.jobHolder.deleteJob(job)
}

func (tube *tube) bury(job *job) {
	job.jobHolder.buryJob(job)
	job.state = jobBuriedState
	job.buryCount += 1
	job.client = nil
}

func (tube *tube) touch(job *job) {
	job.jobHolder.touchJob(job)
}

func (tube *tube) pause(duration time.Duration) {
	tube.paused = true
	tube.pauseStartedAt = time.Now()
	tube.pauseEndsAt = time.Now().Add(duration)
	time.Sleep(duration)
}

func (tube *tube) kick(bound int) (actual int) {
	if tube.buried.Len() > 0 {
		actual = tube.buried.kickJobs(bound)
	} else {
		actual = tube.delayed.kickJobs(bound)
	}

	return
}

func (tube *tube) peek(request *jobPeekRequest) {
	switch request.state {
	case jobReadyState:
		tube.ready.peekJob(request)
	case jobBuriedState:
		tube.buried.peekJob(request)
	case jobDelayedState:
		tube.delayed.peekJob(request)
	default:
		panic("Unknown jobPeekRequest: " + request.state)
	}
}
