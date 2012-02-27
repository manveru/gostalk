package gostalker

import (
  "strconv"
  "time"
  "io"
)

type Cmd struct {
  server *Server
  client *Client
  name string
  args []string
}

func (cmd *Cmd) bury() (res Response) {
  return
}
func (cmd *Cmd) delete() (res Response) {
  return
}
func (cmd *Cmd) ignore() (res Response) {
  return
}
func (cmd *Cmd) kick() (res Response) {
  return
}
func (cmd *Cmd) listTubes() (res Response) {
  return
}
func (cmd *Cmd) listTubesWatched() (res Response) {
  return
}
func (cmd *Cmd) listTubeUsed() (res Response) {
  return
}
func (cmd *Cmd) pauseTube() (res Response) {
  return
}
func (cmd *Cmd) peek() (res Response) {
  return
}
func (cmd *Cmd) peekBuried() (res Response) {
  return
}
func (cmd *Cmd) peekDelayed() (res Response) {
  return
}
func (cmd *Cmd) peekReady() (res Response) {
  return
}
func (cmd *Cmd) put() (res Response) {
  if len(cmd.args) < 3 { return BAD_FORMAT }

  priority, err := strconv.Atoi(cmd.args[0])
  if err != nil { cmd.server.logf("strconv.Atoi", err); return BAD_FORMAT }

  delay, err := strconv.Atoi(cmd.args[1])
  if err != nil { cmd.server.logf("strconv.Atoi", err); return BAD_FORMAT }

  ttr, err := strconv.Atoi(cmd.args[2])
  if err != nil { cmd.server.logf("strconv.Atoi", err); return BAD_FORMAT }

  bodySize, err := strconv.Atoi(cmd.args[3])
  if err != nil { cmd.server.logf("strconv.Atoi", err); return BAD_FORMAT }
  if bodySize > JOB_DATA_SIZE_LIMIT { return JOB_TOO_BIG }

  body := make([]byte, bodySize + 2)
  _, err = io.ReadFull(cmd.client.reader, body)
  if err != nil { cmd.server.logf("io.ReadFull", err); return INTERNAL_ERROR }

  id := <-cmd.server.getJobId
  job := &Job{id, priority, time.Now(), time.Duration(delay), time.Duration(ttr), body[0:len(body)-2]}

  return
}
func (cmd *Cmd) quit() (res Response) {
  return
}
func (cmd *Cmd) reserve() (res Response) {
  return
}
func (cmd *Cmd) stats() (res Response) {
  return
}
func (cmd *Cmd) statsJob() (res Response) {
  return
}
func (cmd *Cmd) statsTube() (res Response) {
  return
}
func (cmd *Cmd) touch() (res Response) {
  return
}
func (cmd *Cmd) use() (res Response) {
  return
}
func (cmd *Cmd) watch() (res Response) {
  return
}
