package gostalk

import (
	"time"
)

type jobReserveRequest struct {
	client  *Client
	success chan *Job
	cancel  chan bool
}

type jobKickRequest struct {
	bound   int
	success chan int
}

type Tube struct {
	name     string
	ready    *readyJobs
	reserved *reservedJobs
	buried   *buriedJobs
	delayed  *delayedJobs

	jobDemand chan *jobReserveRequest
	jobSupply chan *Job
	jobDelete chan *Job
	jobTouch  chan *Job
	jobBury   chan *Job
	jobKick   chan *jobKickRequest
	tubePause chan time.Duration

	paused         bool
	pauseStartedAt time.Time
	pauseEndsAt    time.Time

	stats *tubeStats
}

func newTube(name string) (tube *Tube) {
	tube = &Tube{
		name:      name,
		paused:    false,
		ready:     newReadyJobs(),
		reserved:  newReservedJobs(),
		buried:    newBuriedJobs(),
		delayed:   newDelayedJobs(),
		jobDemand: make(chan *jobReserveRequest),
		jobSupply: make(chan *Job),
		jobDelete: make(chan *Job),
		jobTouch:  make(chan *Job),
		jobBury:   make(chan *Job),
		jobKick:   make(chan *jobKickRequest),
		stats:     &tubeStats{Name: name},
	}

	go tube.handleDemand()

	return
}

func (tube *Tube) handleDemand() {
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
			case request := <-tube.jobKick:
				request.success <- tube.kick(request.bound)
			case job := <-tube.jobSupply:
				tube.put(job)
			}
		}
	}
}

func (tube *Tube) reserve(client *Client) (job *Job) {
	job = tube.ready.getJob()

	tube.reserved.putJob(job)

	job.client = client
	job.state = jobReservedState
	job.reserveCount += 1
	job.reserveEndsAt = time.Now().Add(job.timeToReserve)

	return
}

func (tube *Tube) put(job *Job) {
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

func (tube *Tube) delete(job *Job) {
	if job.isUrgent() {
		tube.stats.CurrentUrgentJobs -= 1
	}
	job.jobHolder.deleteJob(job)
}

func (tube *Tube) bury(job *Job) {
	job.jobHolder.buryJob(job)
	job.state = jobBuriedState
	job.buryCount += 1
	job.client = nil
}

func (tube *Tube) touch(job *Job) {
	job.jobHolder.touchJob(job)
}

func (tube *Tube) pause(duration time.Duration) {
	tube.paused = true
	tube.pauseStartedAt = time.Now()
	tube.pauseEndsAt = time.Now().Add(duration)
	time.Sleep(duration)
}

func (tube *Tube) kick(bound int) (actual int) {
	if tube.buried.Len() > 0 {
		actual = tube.buried.kickJobs(bound)
	} else {
		actual = tube.delayed.kickJobs(bound)
	}

	return
}
