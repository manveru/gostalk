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
  delayed  time.Duration
  reserved time.Duration
  body     []byte
}

func (job Job) reservedString() string {
  return fmt.Sprintf("RESERVED %d %d\r\n%s\r\n", job.id, len(job.body), job.body)
}
