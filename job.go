package gostalk

import (
  "fmt"
  "time"
)

const (
  jobReadyState    = "ready"
  jobDelayedState  = "delayed"
  jobReservedState = "reserved"
  jobBuriedState   = "buried"
)

type JobHolder interface {
  deleteJob(*Job)
  touchJob(*Job)
}

type JobId uint64
type Job struct {
  id       JobId
  priority uint32
  state    string
  body     []byte
  tube     *Tube
  index    int

  createdAt     time.Time
  delayEndsAt   time.Time
  reserveEndsAt time.Time
  timeToReserve time.Duration

  reserveCount int
  releaseCount int
  timeoutCount int
  buryCount    int
  kickCount    int

  jobHolder JobHolder
}

func newJob(id JobId, priority uint32, delay uint64, ttr uint64, body []byte) (job *Job) {
  job = &Job{
    id:            id,
    priority:      priority,
    createdAt:     time.Now(),
    timeToReserve: time.Duration(ttr),
    body:          body,
  }

  if delay > 0 {
    job.delayEndsAt = time.Now().Add(time.Duration(delay) * time.Second)
  }

  return job
}

func (job Job) reservedString() string {
  return fmt.Sprintf("RESERVED %d %d\r\n%s\r\n", job.id, len(job.body), job.body)
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

func (job *Job) touch() {
  job.tube.jobTouch <- job
}
