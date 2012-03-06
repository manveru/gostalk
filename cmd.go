package gostalk

import (
	"fmt"
	"io"
	"strconv"
	"time"
)

type cmd struct {
	server      *Server
	client      *Client
	name        string
	args        []string
	respondChan chan string
	closeConn   chan bool
}

func newCmd(name string, args []string) *cmd {
	return &cmd{
		respondChan: make(chan string),
		closeConn:   make(chan bool),
		name:        name,
		args:        args,
	}
}

func (cmd *cmd) respond(res string) {
	cmd.respondChan <- res
}

func (cmd *cmd) assertNumberOfArguments(n int) {
	if len(cmd.args) != n {
		pf("Wrong number of arguments: expected %d, got %d", n, len(cmd.args))
		cmd.respond(MSG_BAD_FORMAT)
	}
}

func (cmd *cmd) getInt(idx int) (to int64) {
	from := cmd.args[idx]
	to, err := strconv.ParseInt(from, 10, 64)
	if err != nil {
		pf("cmd.getInt(%#v) : %v", from, err)
		cmd.respond(MSG_BAD_FORMAT)
	}
	return
}

func (cmd *cmd) getUint(idx int) (to uint64) {
	from := cmd.args[idx]
	to, err := strconv.ParseUint(from, 10, 64)
	if err != nil {
		pf("cmd.getInt(%#v) : %v", from, err)
		cmd.respond(MSG_BAD_FORMAT)
	}
	return
}

func (cmd *cmd) bury() {
	cmd.assertNumberOfArguments(1)
	jobId := JobId(cmd.getUint(0))

	job, found := cmd.server.jobs[jobId]
	if found && job.state == jobReservedState && job.client == cmd.client {
		job.bury()
		cmd.respond(MSG_BURIED)
	} else {
		cmd.respond(MSG_NOT_FOUND)
		return
	}
}

func (cmd *cmd) delete() {
	cmd.assertNumberOfArguments(1)
	jobId := JobId(cmd.getUint(0))

	job, found := cmd.server.jobs[jobId]
	if !found {
		cmd.respond(MSG_NOT_FOUND)
		return
	}

	switch job.state {
	case jobReservedState:
		if job.client == cmd.client {
			job.deleteFrom(cmd.server)
			cmd.respond(MSG_DELETED)
		} else {
			cmd.respond(MSG_NOT_FOUND)
		}
	case jobBuriedState, jobDelayedState, jobReadyState:
		job.deleteFrom(cmd.server)
		cmd.respond(MSG_DELETED)
	default:
		cmd.respond(MSG_NOT_FOUND)
	}
}

func (cmd *cmd) ignore() {
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

func (cmd *cmd) kick() {
	cmd.assertNumberOfArguments(1)
	bound := cmd.getUint(0)

	jobKick := &jobKickRequest{
		bound:   int(bound),
		success: make(chan int),
	}

	cmd.client.usedTube.jobKick <- jobKick
	actual := <-jobKick.success

	cmd.respond(fmt.Sprintf("KICKED %d\r\n", actual))
}

func (cmd *cmd) listTubes() {
	cmd.assertNumberOfArguments(0)

	list := make([]string, 0)
	for key := range cmd.server.tubes {
		list = append(list, key)
	}

	yaml, err := toYaml(list)
	if err != nil {
		p(err)
		cmd.respond(MSG_INTERNAL_ERROR)
	}

	cmd.respond(fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml))
}

func (cmd *cmd) listTubesWatched() {
	cmd.assertNumberOfArguments(0)

	list := make([]string, 0)
	for _, tube := range cmd.client.watchedTubes {
		list = append(list, tube.name)
	}

	yaml, err := toYaml(list)
	if err != nil {
		p(err)
		cmd.respond(MSG_INTERNAL_ERROR)
	}

	cmd.respond(fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml))
}
func (cmd *cmd) listTubeUsed() {
	cmd.assertNumberOfArguments(0)
	cmd.respond(fmt.Sprintf("USING %s\r\n", cmd.client.usedTube.name))
}
func (cmd *cmd) pauseTube() {
	cmd.assertNumberOfArguments(2)

	tube, found := cmd.server.findTube(cmd.args[0])
	if !found {
		cmd.respond(MSG_NOT_FOUND)
		return
	}

	delay := cmd.getInt(1)
	tube.tubePause <- time.Duration(delay) * time.Second
	cmd.respond(MSG_PAUSED)
}
func (cmd *cmd) peek() {
	cmd.respond(MSG_INTERNAL_ERROR)
}
func (cmd *cmd) peekBuried() {
	cmd.respond(MSG_INTERNAL_ERROR)
}
func (cmd *cmd) peekDelayed() {
	cmd.respond(MSG_INTERNAL_ERROR)
}
func (cmd *cmd) peekReady() {
	cmd.respond(MSG_INTERNAL_ERROR)
}

func (cmd *cmd) put() {
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

	tube := cmd.client.usedTube

	id := <-cmd.server.getJobId
	job := newJob(id, priority, delay, ttr, body)

	tube.jobSupply <- job
	cmd.server.jobs[job.id] = job
	cmd.client.isProducer = true
	cmd.respond(fmt.Sprintf("INSERTED %d\r\n", job.id))
}

func (cmd *cmd) quit() {
	cmd.assertNumberOfArguments(0)
	cmd.closeConn <- true
}

func (cmd *cmd) reserveCommon() *jobReserveRequest {
	request := &jobReserveRequest{
		client:  cmd.client,
		success: make(chan *Job),
		cancel:  make(chan bool, 1),
	}

	for _, tube := range cmd.client.watchedTubes {
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

func (cmd *cmd) reserve() {
	cmd.assertNumberOfArguments(0)

	cmd.client.isWorker = true
	request := cmd.reserveCommon()
	job := <-request.success
	request.cancel <- true
	cmd.respond(job.reservedString())
}

func (cmd *cmd) reserveWithTimeout() {
	cmd.assertNumberOfArguments(1) // seconds
	seconds := cmd.getInt(0)
	if seconds < 0 {
		seconds = 0
	}

	cmd.client.isWorker = true
	request := cmd.reserveCommon()

	select {
	case job := <-request.success:
		cmd.respond(job.reservedString())
		request.cancel <- true
	case <-time.After(time.Duration(seconds) * time.Second):
		cmd.respond(MSG_TIMED_OUT)
		request.cancel <- true
	}
}

func (cmd *cmd) stats() {
	cmd.assertNumberOfArguments(0)

	stats := cmd.server.statistics()
	p(stats)
	yaml, err := toYaml(stats)
	if err != nil {
		p(err)
		cmd.respond(MSG_INTERNAL_ERROR)
		return
	}

	cmd.respond(fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml))
}

func (cmd *cmd) statsJob() {
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

	yaml, err := toYaml(stats)
	if err != nil {
		p(err)
		cmd.respond(MSG_INTERNAL_ERROR)
		return
	}

	cmd.respond(fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml))
}
func (cmd *cmd) statsTube() {
	cmd.assertNumberOfArguments(1)

	tube, found := cmd.server.findTube(cmd.args[0])

	if !found {
		cmd.respond(MSG_NOT_FOUND)
		return
	}

	stats := tube.statistics()

	yaml, err := toYaml(stats)
	if err != nil {
		p(err)
		cmd.respond(MSG_INTERNAL_ERROR)
		return
	}

	cmd.respond(fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml))
}
func (cmd *cmd) touch() {
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

func (cmd *cmd) use() {
	cmd.assertNumberOfArguments(1)

	name := cmd.args[0]
	if !NAME_CHARS.MatchString(name) {
		cmd.respond(MSG_BAD_FORMAT)
	}

	cmd.client.usedTube = cmd.server.findOrCreateTube(name)
	cmd.respond(fmt.Sprintf("USING %s\r\n", name))
}

func (cmd *cmd) watch() {
	cmd.assertNumberOfArguments(1)

	name := cmd.args[0]
	if !NAME_CHARS.MatchString(name) {
		cmd.respond(MSG_BAD_FORMAT)
	}

	cmd.client.watchTube(name)
	cmd.respond("OK\r\n")
}
