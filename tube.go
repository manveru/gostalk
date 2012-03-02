package gostalk

import (
  "container/heap"
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

  statUrgent   int
  statReady    int
  statReserved int
  statDelayed  int
  statBuried   int
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
      case job := <-tube.jobSupply:
        tube.put(job)
      }
    }
  }
}

func (tube *Tube) reserve() (job *Job) {
  job = heap.Pop(tube.ready).(*Job)
  tube.statReady = tube.ready.Len()
  heap.Push(tube.reserved, job)
  tube.statReserved = tube.reserved.Len()
  return
}

func (tube *Tube) put(job *Job) {
  heap.Push(tube.ready, job)
  job.state = jobReady
  job.tube = tube
  tube.statReady = tube.ready.Len()
  if job.priority < 1024 {
    tube.statUrgent += 1
  }
}

func (tube *Tube) delete(job *Job) {
}
