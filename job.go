package gostalk

import (
  "fmt"
  "time"
)

type JobId uint64
type Job struct {
  id       JobId
  priority uint32
  body     []byte

  createdAt time.Time
  delayEndsAt time.Time
  reserveEndsAt time.Time
  timeToReserve time.Duration
}

func newJob(id JobId, priority uint32, delay uint64, ttr uint64, body []byte) (job *Job) {
  job = &Job{
    id:       id,
    priority: priority,
    createdAt:  time.Now(),
    timeToReserve:      time.Duration(ttr),
    body:     body,
  }

  if delay > 0 {
    job.delayEndsAt = time.Now().Add(time.Duration(delay) * time.Second)
  }

  return job
}

func (job Job) reservedString() string {
  return fmt.Sprintf("RESERVED %d %d\r\n%s\r\n", job.id, len(job.body), job.body)
}

// func (job *Job) reserve
