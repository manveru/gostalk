package gostalk

import (
  "fmt"
  "io"
  "launchpad.net/goyaml"
  "runtime"
  "strconv"
  "syscall"
  "time"
)

type Cmd struct {
  server      *Server
  client      *Client
  name        string
  args        []string
  respondChan chan string
  closeConn   chan bool
}

func newCmd(name string, args []string) *Cmd {
  return &Cmd{
    respondChan: make(chan string),
    closeConn:   make(chan bool),
    name:        name,
    args:        args,
  }
}

func (cmd *Cmd) respond(res string) {
  cmd.respondChan <- res
}

func (cmd *Cmd) assertNumberOfArguments(n int) {
  if len(cmd.args) != n {
    pf("Wrong number of arguments: expected %d, got %d", n, len(cmd.args))
    cmd.respond(BAD_FORMAT)
  }
}

func (cmd *Cmd) getInt(idx int) (to uint64) {
  from := cmd.args[idx]
  to, err := strconv.ParseUint(from, 10, 64)
  if err != nil {
    pf("cmd.getInt(%#v) : %v", from, err)
    cmd.respond(BAD_FORMAT)
  }
  return
}

func (cmd *Cmd) bury() {
  cmd.respond(INTERNAL_ERROR)
}

func (cmd *Cmd) delete() {
  cmd.respond(INTERNAL_ERROR)
}

func (cmd *Cmd) ignore() {
  cmd.assertNumberOfArguments(1)

  name := cmd.args[0]
  if !NAME_CHARS.MatchString(name) {
    cmd.respond(BAD_FORMAT)
  }

  ignored, totalTubes := cmd.client.ignoreTube(name)
  if ignored {
    cmd.respond(fmt.Sprintf("WATCHING %d\r\n", totalTubes))
  } else {
    cmd.respond("NOT_IGNORED\r\n")
  }
}

func (cmd *Cmd) kick() {
  cmd.respond(INTERNAL_ERROR)
}

func (cmd *Cmd) listTubes() {
  cmd.assertNumberOfArguments(0)

  list := make([]string, 0)
  for key := range cmd.server.tubes {
    list = append(list, key)
  }

  yaml, err := goyaml.Marshal(list)
  if err != nil {
    pf("goyaml.Marshal : %#v", err)
    cmd.respond(INTERNAL_ERROR)
  }

  cmd.respond(fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml))
}

func (cmd *Cmd) listTubesWatched() {
  cmd.assertNumberOfArguments(0)

  list := make([]string, 0)
  for _, tube := range cmd.client.watchedTubes {
    list = append(list, tube.name)
  }

  yaml, err := goyaml.Marshal(list)
  if err != nil {
    pf("goyaml.Marshal : %#v", err)
    cmd.respond(INTERNAL_ERROR)
  }

  cmd.respond(fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml))
}
func (cmd *Cmd) listTubeUsed() {
  cmd.assertNumberOfArguments(0)
  cmd.respond(fmt.Sprintf("USING %s\r\n", cmd.client.usedTube.name))
}
func (cmd *Cmd) pauseTube() {
  cmd.respond(INTERNAL_ERROR)
}
func (cmd *Cmd) peek() {
  cmd.respond(INTERNAL_ERROR)
}
func (cmd *Cmd) peekBuried() {
  cmd.respond(INTERNAL_ERROR)
}
func (cmd *Cmd) peekDelayed() {
  cmd.respond(INTERNAL_ERROR)
}
func (cmd *Cmd) peekReady() {
  cmd.respond(INTERNAL_ERROR)
}

func (cmd *Cmd) put() {
  cmd.assertNumberOfArguments(4)

  priority := uint32(cmd.getInt(0))
  if priority < 0 {
    priority = 0
  } else if priority > 4294967295 {
    priority = 4294967295
  }
  delay := cmd.getInt(1)
  ttr := cmd.getInt(2)
  if ttr < 1 {
    ttr = 1
  }
  bodySize := cmd.getInt(3)

  if bodySize > JOB_DATA_SIZE_LIMIT {
    cmd.respond(JOB_TOO_BIG)
  }

  body := make([]byte, bodySize)
  _, err := io.ReadFull(cmd.client.reader, body)
  if err != nil {
    pf("io.ReadFull : %#v", err)
    cmd.respond(INTERNAL_ERROR)
  }
  rn := make([]byte, 2)
  _, err = io.ReadAtLeast(cmd.client.reader, rn, 2)
  if err != nil {
    if err.Error() == "ErrUnexpextedEOF" {
      cmd.respond(EXPECTED_CRLF)
    } else {
      pf("io.ReadAtLeast : %#v", err)
      cmd.respond(INTERNAL_ERROR)
    }
  }

  if rn[0] != '\r' || rn[1] != '\n' {
    cmd.respond(EXPECTED_CRLF)
  }

  id := <-cmd.server.getJobId
  job := newJob(id, priority, delay, ttr, body)

  tube := cmd.client.usedTube
  tube.jobSupply <- job
  cmd.client.isProducer = true
  cmd.respond(fmt.Sprintf("INSERTED %d\r\n", job.id))
}

func (cmd *Cmd) quit() {
  cmd.assertNumberOfArguments(0)
  cmd.closeConn <- true
}

func reserveCommon(tubes map[string]*Tube) *jobReserveRequest {
  request := &jobReserveRequest{
    success: make(chan *Job),
    cancel:  make(chan bool, 1),
  }

  for _, tube := range tubes {
    go func(t *Tube, r *jobReserveRequest) {
      select {
      case t.jobDemand <- r:
      case <-r.cancel:
        r.cancel <- true
      }
    }(tube, request)
  }

  return request
}

func (cmd *Cmd) reserve() {
  cmd.assertNumberOfArguments(0)

  cmd.client.isWorker = true
  request := reserveCommon(cmd.client.watchedTubes)
  job := <-request.success
  request.cancel <- true
  cmd.respond(job.reservedString())
}

func (cmd *Cmd) reserveWithTimeout() {
  cmd.assertNumberOfArguments(1) // seconds
  seconds := cmd.getInt(0)
  if seconds < 0 {
    seconds = 0
  }

  cmd.client.isWorker = true
  request := reserveCommon(cmd.client.watchedTubes)

  select {
  case job := <-request.success:
    cmd.respond(job.reservedString())
    request.cancel <- true
  case <-time.After(time.Duration(seconds) * time.Second):
    cmd.respond(TIMED_OUT)
    request.cancel <- true
  }
}

func (cmd *Cmd) stats() {
  cmd.assertNumberOfArguments(0)
  server := cmd.server
  urgent, ready, reserved, delayed, buried := server.statJobs()

  raw := map[string]interface{}{
    "version":               GOSTALK_VERSION,
    "total-connections":     server.totalConnectionCount,
    "current-connections":   server.currentConnectionCount,
    "pid":                   server.pid,
    "uptime":                time.Since(cmd.server.startedAt).Seconds(),
    "max-job-size":          JOB_DATA_SIZE_LIMIT,
    "current-tubes":         len(server.tubes),
    "current-jobs-urgent":   urgent,
    "current-jobs-ready":    ready,
    "current-jobs-reserved": reserved,
    "current-jobs-delayed":  delayed,
    "current-jobs-buried":   buried,
    "go-current-goroutines": runtime.NumGoroutine(),
  }

  for key, value := range server.commandUsage {
    raw["cmd-"+key] = value
  }

  usage := new(syscall.Rusage)
  err := syscall.Getrusage(syscall.RUSAGE_SELF, usage)
  if err == nil {
    utimeSec, utimeNsec := usage.Utime.Unix()
    stimeSec, stimeNsec := usage.Stime.Unix()
    raw["rusage-utime"] = float32(utimeSec) + (float32(utimeNsec) / 10000000.0)
    raw["rusage-stime"] = float32(stimeSec) + (float32(stimeNsec) / 10000000.0)
  } else {
    pf("failed to get rusage : %v", err)
  }

  /*
     TODO: those still need implementation
     raw["job-timeouts"] = 1234 // is the cumulative count of times a job has timed out.
     raw["total-jobs"] = 1234 // is the cumulative count of jobs created.
     raw["current-producers"] = 1234 // is the number of open connections that have each
     raw["current-workers"] = 1234 // is the number of open connections that have each issued
     raw["current-waiting"] = 1234 // is the number of open connections that have issued a
     raw["binlog-oldest-index"] = 1234 // is the index of the oldest binlog file needed to
     raw["binlog-current-index"] = 1234 // is the index of the current binlog file being
     raw["binlog-max-size"] = 1234 // is the maximum size in bytes a binlog file is allowed
     raw["binlog-records-written"] = 1234 // is the cumulative number of records written
     raw["binlog-records-migrated"] = 1234 // is the cumulative number of records written
  */

  yaml, err := goyaml.Marshal(raw)
  if err != nil {
    pf("goyaml.Marshal : %#v", err)
    cmd.respond(INTERNAL_ERROR)
  }
  cmd.respond(string(yaml))
}
func (cmd *Cmd) statsJob() {
  cmd.respond(INTERNAL_ERROR)
}
func (cmd *Cmd) statsTube() {
  cmd.respond(INTERNAL_ERROR)
}
func (cmd *Cmd) touch() {
  cmd.respond(INTERNAL_ERROR)
}

func (cmd *Cmd) use() {
  cmd.assertNumberOfArguments(1)

  name := cmd.args[0]
  if !NAME_CHARS.MatchString(name) {
    cmd.respond(BAD_FORMAT)
  }

  cmd.client.usedTube = cmd.server.findOrCreateTube(name)
  cmd.respond(fmt.Sprintf("USING %s\r\n", name))
}

func (cmd *Cmd) watch() {
  cmd.assertNumberOfArguments(1)

  name := cmd.args[0]
  if !NAME_CHARS.MatchString(name) {
    cmd.respond(BAD_FORMAT)
  }

  cmd.client.watchTube(name)
  cmd.respond("OK\r\n")
}
