package gostalk

import (
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

type server struct {
	getJobId  chan jobId
	jobs      map[jobId]*job
	tubes     map[string]*tube
	startedAt time.Time
	stats     *serverStats
}

func newServer() *server {
	s := &server{
		getJobId:  make(chan jobId, 42),
		tubes:     make(map[string]*tube),
		jobs:      make(map[jobId]*job),
		startedAt: time.Now(),
		stats: &serverStats{
			Version:    GOSTALK_VERSION,
			PID:        os.Getpid(),
			MaxJobSize: JOB_DATA_SIZE_LIMIT,
		},
	}

	go s.runGetJobId()

	return s
}

func (server *server) runGetJobId() {
	var n jobId
	for {
		server.getJobId <- n
		n = n + 1
	}
}

// TODO: get rid of unused tubes.
func (server *server) findOrCreateTube(name string) *tube {
	tube, found := server.findTube(name)

	if !found {
		tube = newTube(name)
		server.tubes[name] = tube
	}

	return tube
}

func (server *server) findJob(id jobId) (job *job, found bool) {
	job, found = server.jobs[id]
	return
}

func (server *server) findTube(name string) (tube *tube, found bool) {
	tube, found = server.tubes[name]
	return
}

func (server *server) accept(conn conn) {
	defer server.acceptFinalize(conn)
	atomic.AddInt64(&server.stats.CurrentConnections, 1)
	atomic.AddInt64(&server.stats.TotalConnections, 1)

	client := newClient(server, conn)

	for {
		err := processCommand(server, client)
		if err != nil {
			pf("Error in processCommand: %#v", err)
			return
		}
	}
}

func (server *server) acceptFinalize(conn conn) {
	if x := recover(); x != nil {
		pf("runtime panic: %v\n", x)
		debug.PrintStack()
	}

	pf("Closing Connection: %#v", conn)
	conn.Close()
	atomic.AddInt64(&server.stats.CurrentConnections, -1)
}

func processCommand(server *server, client *client) (err error) {
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
		response = MSG_UNKNOWN_COMMAND
	case response = <-cmd.respondChan:
	case <-cmd.closeConn:
		return exception("Close Connection")
	}

	pf("← %#v", response)
	client.conn.Write([]byte(response))
	return
}

func readCommand(reader reader) (cmd *cmd, err error) {
	line, _, err := reader.ReadLine()
	if err == nil {
		chunks := strings.Fields(string(line))
		cmd = newCmd(chunks[0], chunks[1:])
	}

	return
}

func executeCommand(cmd *cmd, unknownCommandChan chan bool) {
	switch cmd.name {
	case "bury":
		atomic.AddInt64(&cmd.server.stats.CmdBury, 1)
		cmd.bury()
	case "delete":
		atomic.AddInt64(&cmd.server.stats.CmdDelete, 1)
		cmd.delete()
	case "ignore":
		atomic.AddInt64(&cmd.server.stats.CmdIgnore, 1)
		cmd.ignore()
	case "kick":
		atomic.AddInt64(&cmd.server.stats.CmdKick, 1)
		cmd.kick()
	case "list-tubes":
		atomic.AddInt64(&cmd.server.stats.CmdListTubes, 1)
		cmd.listTubes()
	case "list-tubes-watched":
		atomic.AddInt64(&cmd.server.stats.CmdListTubesWatched, 1)
		cmd.listTubesWatched()
	case "list-tube-used":
		atomic.AddInt64(&cmd.server.stats.CmdListTubeUsed, 1)
		cmd.listTubeUsed()
	case "pause-tube":
		atomic.AddInt64(&cmd.server.stats.CmdPauseTube, 1)
		cmd.pauseTube()
	case "peek-buried":
		atomic.AddInt64(&cmd.server.stats.CmdPeekBuried, 1)
		cmd.peekBuried()
	case "peek":
		atomic.AddInt64(&cmd.server.stats.CmdPeek, 1)
		cmd.peek()
	case "peek-delayed":
		atomic.AddInt64(&cmd.server.stats.CmdPeekDelayed, 1)
		cmd.peekDelayed()
	case "peek-ready":
		atomic.AddInt64(&cmd.server.stats.CmdPeekReady, 1)
		cmd.peekReady()
	case "put":
		atomic.AddInt64(&cmd.server.stats.CmdPut, 1)
		cmd.put()
	case "quit":
		atomic.AddInt64(&cmd.server.stats.CmdQuit, 1)
		cmd.quit()
	case "reserve":
		atomic.AddInt64(&cmd.server.stats.CmdReserve, 1)
		cmd.reserve()
	case "reserve-with-timeout":
		atomic.AddInt64(&cmd.server.stats.CmdReserveWithTimeout, 1)
		cmd.reserveWithTimeout()
	case "stats":
		atomic.AddInt64(&cmd.server.stats.CmdStats, 1)
		cmd.stats()
	case "stats-job":
		atomic.AddInt64(&cmd.server.stats.CmdStatsJob, 1)
		cmd.statsJob()
	case "stats-tube":
		atomic.AddInt64(&cmd.server.stats.CmdStatsTube, 1)
		cmd.statsTube()
	case "touch":
		atomic.AddInt64(&cmd.server.stats.CmdTouch, 1)
		cmd.touch()
	case "use":
		atomic.AddInt64(&cmd.server.stats.CmdUse, 1)
		cmd.use()
	case "watch":
		atomic.AddInt64(&cmd.server.stats.CmdWatch, 1)
		cmd.watch()
	default:
		unknownCommandChan <- true
	}
}

func (server *server) exit(status int) {
	os.Exit(status)
}

func (server *server) exitOn(name string, err error) {
	if err != nil {
		pf("Exit in %s: %v", name, err)
		server.exit(1)
	}
}
