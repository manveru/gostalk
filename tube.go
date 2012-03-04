package gostalk

import (
  "time"
)

type jobReserveRequest struct {
  success chan *Job
  cancel  chan bool
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

  pauseStartedAt time.Time
  pauseEndsAt    time.Time

  statUrgent    int
  statReady     int
  statReserved  int
  statDelayed   int
  statBuried    int
  statTotalJobs int
  statDeleted   int
  statPaused    int
}

func newTube(name string) (tube *Tube) {
  tube = &Tube{
    name:      name,
    ready:     newReadyJobs(),
    reserved:  newReservedJobs(),
    buried:    newBuriedJobs(),
    delayed:   newDelayedJobs(),
    jobDemand: make(chan *jobReserveRequest),
    jobSupply: make(chan *Job),
    jobDelete: make(chan *Job),
    jobTouch:  make(chan *Job),
  }

  go tube.handleDemand()

  return
}

func (tube *Tube) handleDemand() {
  for {
    if tube.ready.Len() > 0 {
      select {
      case job := <-tube.jobDelete:
        tube.delete(job)
      case job := <-tube.jobSupply:
        tube.put(job)
      case job := <-tube.jobTouch:
        tube.touch(job)
      case request := <-tube.jobDemand:
        select {
        case request.success <- tube.reserve():
        case <-request.cancel:
          request.cancel <- true // propagate to the other tubes
        }
      }
    } else {
      select {
      case job := <-tube.jobDelete:
        tube.delete(job)
      case job := <-tube.jobTouch:
        tube.touch(job)
      case job := <-tube.jobSupply:
        tube.put(job)
      }
    }
  }
}

func (tube *Tube) reserve() (job *Job) {
  job = tube.ready.getJob()
  tube.statReady = tube.ready.Len()
  tube.reserved.putJob(job)

  job.state = jobReservedState
  job.reserveEndsAt = time.Now().Add(job.timeToReserve)
  job.jobHolder = tube.reserved
  job.reserveCount += 1

  tube.statReserved = tube.reserved.Len()
  return
}

func (tube *Tube) put(job *Job) {
  tube.ready.putJob(job)

  job.state = jobReadyState
  job.tube = tube
  job.jobHolder = tube.ready

  tube.statReady = tube.ready.Len()
  if job.priority < 1024 {
    tube.statUrgent += 1
  }
}

func (tube *Tube) delete(job *Job) {
  job.jobHolder.deleteJob(job)
}

func (tube *Tube) touch(job *Job) {
  job.jobHolder.touchJob(job)
}

func (tube *Tube) pauseTimeLeft() (seconds time.Duration) {
  return
}

func (tube *Tube) pausedDuration() (seconds time.Duration) {
  return
}
