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
		err := processCommand(client)
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

func processCommand(client *client) (err error) {
	name, args, err := readCommand(client.reader)

	if err != nil {
		p("readCommand", err)
		return
	}

	handler, found := commands[name]

	if found {
		response := handler(client, args)
		client.conn.Write([]byte(response))
	} else {
		client.conn.Write([]byte(MSG_UNKNOWN_COMMAND))
	}

	return
}

func readCommand(reader reader) (name string, arguments args, err error) {
	line, _, err := reader.ReadLine()
	if err == nil {
		chunks := strings.Fields(string(line))
		return chunks[0], args(chunks[1:]), err
	}

	return
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
