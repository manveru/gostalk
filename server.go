package gostalker

import (
  "bufio"
  "strings"
  "runtime/debug"
  "net"
  "os"
)

type Server struct {
  logger Logger
  getJobId chan uint64
}

func (server *Server) runGetJobId() {
  var n uint64
  for {
    server.getJobId <- n
    n = n + 1
  }
}

func (server *Server) accept(conn net.Conn) {
  defer server.acceptFinalize(conn)

  reader := bufio.NewReader(conn)
  client := &Client{conn, reader}

  for {
    cmd, err := readCommand(reader)
    server.exitOn("readCommand", err)

    cmd.server = server
    cmd.client = client
    server.logf("cmd: %#v(%#v)", cmd.name, cmd.args)

    response := respond(cmd)

    // magic :( but empty string indicates that we want to close the connection.
    if response == "" {
      return
    } else {
      server.logf("response: %#v", response)
      conn.Write([]byte(response))
    }
  }
}

func (server *Server) acceptFinalize(conn net.Conn) {
  if x := recover(); x != nil {
    server.log("runtime panic: %v\n", x)
    debug.PrintStack()
  }

  server.logf("Closing Connection: %#v", conn)
  conn.Close()
}

func respond(cmd *Cmd) (response Response) {
  switch cmd.name {
  case "bury": response = cmd.bury()
  case "delete": response = cmd.delete()
  case "ignore": response = cmd.ignore()
  case "kick": response = cmd.kick()
  case "list-tubes": response = cmd.listTubes()
  case "list-tubes-watched": response = cmd.listTubesWatched()
  case "list-tube-used": response = cmd.listTubeUsed()
  case "pause-tube": response = cmd.pauseTube()
  case "peek-buried": response = cmd.peekBuried()
  case "peek": response = cmd.peek()
  case "peek-delayed": response = cmd.peekDelayed()
  case "peek-ready": response = cmd.peekReady()
  case "put": response = cmd.put()
  case "quit": response = cmd.quit()
  case "reserve": response = cmd.reserve()
  case "stats": response = cmd.stats()
  case "stats-job": response = cmd.statsJob()
  case "stats-tube": response = cmd.statsTube()
  case "touch": response = cmd.touch()
  case "use": response = cmd.use()
  case "watch": response = cmd.watch()
  default: response = UNKNOWN_COMMAND
  }

  return
}

func readCommand(reader Reader) (cmd *Cmd, err error) {
  bline, _, err := reader.ReadLine()
  if err != nil { return }

  sline := string(bline)
  chunks := strings.Fields(sline)

  cmd = &Cmd{
    name: chunks[0],
    args: chunks[1:],
  }

  return
}

func (server *Server) log(v ...interface{}) {
  server.logger.Println(v...)
}

func (server *Server) logf(format string, v ...interface{}) {
  server.logger.Printf(format, v...)
}

func (server *Server) exit(status int) {
  os.Exit(status)
}

func (server *Server) exitOn(name string, err error) {
  if err != nil {
    server.logf("Exit in %s: %v", name, err)
    server.exit(1)
  }
}
