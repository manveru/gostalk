package gostalker

import (
  "os"
  "runtime/debug"
  "strings"
  "time"
)

type Server struct {
  getJobId               chan JobId
  tubes                  map[string]*Tube
  pid                    int
  startedAt              time.Time
  currentConnectionCount uint
  totalConnectionCount   uint
  commandUsage           map[string]uint
}

func newServer() (server *Server) {
  server = &Server{
    getJobId: make(chan JobId, 42),
    tubes:    make(map[string]*Tube),
    currentConnectionCount: 0,
    totalConnectionCount:   0,
    pid:                    os.Getpid(),
    startedAt:              time.Now(),
    commandUsage: map[string]uint{
      "bury":                 0,
      "delete":               0,
      "ignore":               0,
      "kick":                 0,
      "list-tubes":           0,
      "list-tubes-watched":   0,
      "list-tube-used":       0,
      "pause-tube":           0,
      "peek-buried":          0,
      "peek":                 0,
      "peek-delayed":         0,
      "peek-ready":           0,
      "put":                  0,
      "quit":                 0,
      "reserve":              0,
      "reserve-with-timeout": 0,
      "stats":                0,
      "stats-job":            0,
      "stats-tube":           0,
      "touch":                0,
      "use":                  0,
      "watch":                0,
    },
  }

  go server.runGetJobId()

  return
}

func (server *Server) runGetJobId() {
  var n JobId
  for {
    server.getJobId <- n
    n = n + 1
  }
}

func (server *Server) findOrCreateTube(name string) *Tube {
  found, ok := server.tubes[name]

  if !ok {
    found = newTube(name)
    server.tubes[name] = found
  }

  return found
}

func (server *Server) accept(conn Conn) {
  defer server.acceptFinalize(conn)
  server.totalConnectionCount += 1
  server.currentConnectionCount += 1

  client := newClient(server, conn)

  for {
    err := processCommand(server, client)
    if err != nil {
      pf("Error in processCommand: %#v", err)
      return
    }
  }
}

func (server *Server) acceptFinalize(conn Conn) {
  if x := recover(); x != nil {
    pf("runtime panic: %v\n", x)
    debug.PrintStack()
  }

  pf("Closing Connection: %#v", conn)
  conn.Close()
  server.currentConnectionCount -= 1
}

func processCommand(server *Server, client *Client) (err error) {
  cmd, err := readCommand(client.reader)

  if err != nil {
    p("readCommand", err)
    return
  }

  defer func() {
    close(cmd.closeConn)
    close(cmd.respondChan)
  }()

  cmd.server = server
  cmd.client = client
  pf("→ %#v %#v", cmd.name, cmd.args)

  unknownCommandChan := make(chan bool)
  go executeCommand(cmd, unknownCommandChan)

  response := ""

  select {
  case <-unknownCommandChan:
    response = UNKNOWN_COMMAND
  case response = <-cmd.respondChan:
    cmd.server.commandUsage[cmd.name] += 1
  case <-cmd.closeConn:
    return newError("Close Connection")
  }

  pf("← %#v", response)
  client.conn.Write([]byte(response))
  return
}

func readCommand(reader Reader) (cmd *Cmd, err error) {
  line, _, err := reader.ReadLine()
  if err == nil {
    chunks := strings.Fields(string(line))
    cmd = newCmd(chunks[0], chunks[1:])
  }

  return
}

func executeCommand(cmd *Cmd, unknownCommandChan chan bool) {
  switch cmd.name {
  case "bury":
    cmd.bury()
  case "delete":
    cmd.delete()
  case "ignore":
    cmd.ignore()
  case "kick":
    cmd.kick()
  case "list-tubes":
    cmd.listTubes()
  case "list-tubes-watched":
    cmd.listTubesWatched()
  case "list-tube-used":
    cmd.listTubeUsed()
  case "pause-tube":
    cmd.pauseTube()
  case "peek-buried":
    cmd.peekBuried()
  case "peek":
    cmd.peek()
  case "peek-delayed":
    cmd.peekDelayed()
  case "peek-ready":
    cmd.peekReady()
  case "put":
    cmd.put()
  case "quit":
    cmd.quit()
  case "reserve":
    cmd.reserve()
  case "reserve-with-timeout":
    cmd.reserveWithTimeout()
  case "stats":
    cmd.stats()
  case "stats-job":
    cmd.statsJob()
  case "stats-tube":
    cmd.statsTube()
  case "touch":
    cmd.touch()
  case "use":
    cmd.use()
  case "watch":
    cmd.watch()
  default:
    unknownCommandChan <- true
  }
}

func (server *Server) statJobs() (urgent, ready, reserved, delayed, buried int) {
  for _, tube := range server.tubes {
    urgent += tube.statUrgent
    ready += tube.statReady
    reserved += tube.statReserved
    delayed += tube.statDelayed
    buried += tube.statBuried
  }

  return
}

func (server *Server) exit(status int) {
  os.Exit(status)
}

func (server *Server) exitOn(name string, err error) {
  if err != nil {
    pf("Exit in %s: %v", name, err)
    server.exit(1)
  }
}
