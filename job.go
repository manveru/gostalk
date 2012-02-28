package gostalker

import (
  "time"
)

type Job struct {
  Id       uint64
  Priority int
  Created  time.Time
  Delayed  time.Duration
  Reserved time.Duration
  Body     []byte
}
