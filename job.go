package gostalker

import (
  "fmt"
  "time"
)

type JobId uint64
type Job struct {
  id       JobId
  priority uint32
  created  time.Time
  delay    time.Time
  ttr      time.Duration
  body     []byte
}

func newJob(id JobId, priority uint32, delay uint64, ttr uint64, body []byte) *Job {
  return &Job{
    id:       id,
    priority: priority,
    created:  time.Now(),
    delay:    time.Now().Add(time.Duration(delay) * time.Second),
    ttr:      time.Duration(ttr),
    body:     body,
  }
}

func (job Job) reservedString() string {
  return fmt.Sprintf("RESERVED %d %d\r\n%s\r\n", job.id, len(job.body), job.body)
}
