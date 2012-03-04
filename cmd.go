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
    cmd.respond(MSG_BAD_FORMAT)
  }
}

func (cmd *Cmd) getInt(idx int) (to int64) {
  from := cmd.args[idx]
  to, err := strconv.ParseInt(from, 10, 64)
  if err != nil {
    pf("cmd.getInt(%#v) : %v", from, err)
    cmd.respond(MSG_BAD_FORMAT)
  }
  return
}

func (cmd *Cmd) getUint(idx int) (to uint64) {
  from := cmd.args[idx]
  to, err := strconv.ParseUint(from, 10, 64)
  if err != nil {
    pf("cmd.getInt(%#v) : %v", from, err)
    cmd.respond(MSG_BAD_FORMAT)
  }
  return
}

func (cmd *Cmd) bury() {
  cmd.respond(MSG_INTERNAL_ERROR)
}

func (cmd *Cmd) delete() {
  cmd.assertNumberOfArguments(1)
  jobId := cmd.getInt(0)
  if cmd.server.deleteJob(JobId(jobId)) {
    cmd.respond(MSG_DELETED)
  } else {
    cmd.respond(MSG_NOT_FOUND)
  }
}

func (cmd *Cmd) ignore() {
  cmd.assertNumberOfArguments(1)

  name := cmd.args[0]
  if !NAME_CHARS.MatchString(name) {
    cmd.respond(MSG_BAD_FORMAT)
  }

  ignored, totalTubes := cmd.client.ignoreTube(name)
  if ignored {
    cmd.respond(fmt.Sprintf("WATCHING %d\r\n", totalTubes))
  } else {
    cmd.respond("NOT_IGNORED\r\n")
  }
}

func (cmd *Cmd) kick() {
  cmd.respond(MSG_INTERNAL_ERROR)
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
    cmd.respond(MSG_INTERNAL_ERROR)
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
    cmd.respond(MSG_INTERNAL_ERROR)
  }

  cmd.respond(fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml))
}
func (cmd *Cmd) listTubeUsed() {
  cmd.assertNumberOfArguments(0)
  cmd.respond(fmt.Sprintf("USING %s\r\n", cmd.client.usedTube.name))
}
func (cmd *Cmd) pauseTube() {
  cmd.respond(MSG_INTERNAL_ERROR)
}
func (cmd *Cmd) peek() {
  cmd.respond(MSG_INTERNAL_ERROR)
}
func (cmd *Cmd) peekBuried() {
  cmd.respond(MSG_INTERNAL_ERROR)
}
func (cmd *Cmd) peekDelayed() {
  cmd.respond(MSG_INTERNAL_ERROR)
}
func (cmd *Cmd) peekReady() {
  cmd.respond(MSG_INTERNAL_ERROR)
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
    cmd.respond(MSG_JOB_TOO_BIG)
  }

  body := make([]byte, bodySize)
  _, err := io.ReadFull(cmd.client.reader, body)
  if err != nil {
    pf("io.ReadFull : %#v", err)
    cmd.respond(MSG_INTERNAL_ERROR)
  }
  rn := make([]byte, 2)
  _, err = io.ReadAtLeast(cmd.client.reader, rn, 2)
  if err != nil {
    if err.Error() == "ErrUnexpextedEOF" {
      cmd.respond(MSG_EXPECTED_CRLF)
    } else {
      pf("io.ReadAtLeast : %#v", err)
      cmd.respond(MSG_INTERNAL_ERROR)
    }
  }

  if rn[0] != '\r' || rn[1] != '\n' {
    cmd.respond(MSG_EXPECTED_CRLF)
  }

  id := <-cmd.server.getJobId
  job := newJob(id, priority, delay, ttr, body)

  tube := cmd.client.usedTube
  tube.jobSupply <- job
  cmd.server.jobs[job.id] = job
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
    cmd.respond(MSG_TIMED_OUT)
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
    cmd.respond(MSG_INTERNAL_ERROR)
  }
  // FIXME: needs to send OK <bytes>
  cmd.respond(string(yaml))
}
func (cmd *Cmd) statsJob() {
  cmd.assertNumberOfArguments(1)

  jobId := JobId(cmd.getInt(0))

  job, found := cmd.server.findJob(jobId)

  if !found {
    cmd.respond(MSG_NOT_FOUND)
    return
  }

  stats := &map[string]interface{}{
    "id":        job.id,
    "tube":      job.tube.name,
    "state":     job.state,
    "pri":       job.priority,
    "age":       time.Since(job.createdAt),
    "time-left": job.timeLeft().Seconds(),
    "file":      0, // TODO
    "reserves":  job.reserveCount,
    "releases":  job.releaseCount,
    "timeouts":  job.timeoutCount,
    "buries":    job.buryCount,
    "kicks":     job.kickCount,
  }

  yaml, err := goyaml.Marshal(stats)
  if err != nil {
    pf("goyaml.Marshal : %#v", err)
    cmd.respond(MSG_INTERNAL_ERROR)
    return
  }

  cmd.respond(fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml))
}
func (cmd *Cmd) statsTube() {
  cmd.assertNumberOfArguments(1)

  tube, found := cmd.server.findTube(cmd.args[0])

  if !found {
    cmd.respond(MSG_NOT_FOUND)
    return
  }

  stats := &map[string]interface{}{
    "name":                  tube.name,             // is the tube's name.
    "current-jobs-urgent":   tube.statUrgent,       // is the number of ready jobs with priority < 1024 in this tube.
    "current-jobs-ready":    tube.statReady,        // is the number of jobs in the ready queue in this tube.
    "current-jobs-reserved": tube.statReserved,     // is the number of jobs reserved by all clients in this tube.
    "current-jobs-delayed":  tube.statDelayed,      // is the number of delayed jobs in this tube.
    "current-jobs-buried":   tube.statBuried,       // is the number of buried jobs in this tube.
    "total-jobs":            tube.statTotalJobs,    // is the cumulative count of jobs created in this tube in the current beanstalkd process.
    "current-waiting":       0,                     // TODO: is the number of open connections that have issued a reserve command while watching this tube but not yet received a response.
    "cmd-delete":            tube.statDeleted,      // is the cumulative number of delete commands for this tube
    "cmd-pause-tube":        tube.statPaused,       // is the cumulative number of pause-tube commands for this tube.
    "pause":                 tube.pausedDuration(), // is the number of seconds the tube has been paused for.
    "pause-time-left":       tube.pauseTimeLeft(),  // is the number of seconds until the tube is un-paused.
  }

  yaml, err := goyaml.Marshal(stats)
  if err != nil {
    pf("goyaml.Marshal : %#v", err)
    cmd.respond(MSG_INTERNAL_ERROR)
    return
  }

  cmd.respond(fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml))
}
func (cmd *Cmd) touch() {
  cmd.assertNumberOfArguments(1)
  jobId := JobId(cmd.getInt(0))

  job, found := cmd.server.findJob(jobId)

  if !found {
    cmd.respond(MSG_NOT_FOUND)
    return
  }

  job.touch()
  cmd.respond(MSG_TOUCHED)
}

func (cmd *Cmd) use() {
  cmd.assertNumberOfArguments(1)

  name := cmd.args[0]
  if !NAME_CHARS.MatchString(name) {
    cmd.respond(MSG_BAD_FORMAT)
  }

  cmd.client.usedTube = cmd.server.findOrCreateTube(name)
  cmd.respond(fmt.Sprintf("USING %s\r\n", name))
}

func (cmd *Cmd) watch() {
  cmd.assertNumberOfArguments(1)

  name := cmd.args[0]
  if !NAME_CHARS.MatchString(name) {
    cmd.respond(MSG_BAD_FORMAT)
  }

  cmd.client.watchTube(name)
  cmd.respond("OK\r\n")
}
