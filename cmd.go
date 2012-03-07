package gostalk

import (
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"
)

type cmd struct {
	server      *server
	client      *client
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

var (
	commands = map[string]func(*cmd){
		"bury":                 cmdBury,
		"delete":               cmdDelete,
		"ignore":               cmdIgnore,
		"kick":                 cmdKick,
		"list-tubes":           cmdListTubes,
		"list-tubes-watched":   cmdListTubesWatched,
		"list-tube-used":       cmdListTubeUsed,
		"pause-tube":           cmdPauseTube,
		"peek-buried":          cmdPeekBuried,
		"peek-delayed":         cmdPeekDelayed,
		"peek":                 cmdPeek,
		"peek-ready":           cmdPeekReady,
		"put":                  cmdPut,
		"quit":                 cmdQuit,
		"reserve":              cmdReserve,
		"reserve-with-timeout": cmdReserveWithTimeout,
		"stats-job":            cmdStatsJob,
		"stats":                cmdStats,
		"stats-tube":           cmdStatsTube,
		"touch":                cmdTouch,
		"use":                  cmdUse,
		"watch":                cmdWatch,
	}
)

func cmdBury(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdBury, 1)
	cmd.assertNumberOfArguments(1)
	jobId := jobId(cmd.getUint(0))

	job, found := cmd.server.jobs[jobId]
	if found && job.state == jobReservedState && job.client == cmd.client {
		job.bury()
		cmd.respond(MSG_BURIED)
	} else {
		cmd.respond(MSG_NOT_FOUND)
		return
	}
}

func cmdDelete(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdDelete, 1)
	cmd.assertNumberOfArguments(1)
	jobId := jobId(cmd.getUint(0))

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

func cmdIgnore(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdIgnore, 1)
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

func cmdKick(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdKick, 1)
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

func cmdListTubes(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdListTubes, 1)
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

func cmdListTubesWatched(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdListTubesWatched, 1)
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
func cmdListTubeUsed(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdListTubeUsed, 1)
	cmd.assertNumberOfArguments(0)
	cmd.respond(fmt.Sprintf("USING %s\r\n", cmd.client.usedTube.name))
}

func cmdPauseTube(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdPauseTube, 1)
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

func (cmd *cmd) peekByState(state string) {
	cmd.assertNumberOfArguments(0)
	request := &jobPeekRequest{
		state:   state,
		success: make(chan *job),
	}
	cmd.client.usedTube.jobPeek <- request
	job := <-request.success
	if job == nil {
		cmd.respond(MSG_NOT_FOUND)
	} else {
		cmd.respond(fmt.Sprintf(MSG_PEEK_FOUND, job.id, len(job.body), job.body))
	}
}

func cmdPeek(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdPeek, 1)
	cmd.assertNumberOfArguments(1)
	jobId := jobId(cmd.getInt(0))
	job, found := cmd.server.findJob(jobId)

	if found {
		cmd.respond(fmt.Sprintf(MSG_PEEK_FOUND, job.id, len(job.body), job.body))
	} else {
		cmd.respond(MSG_NOT_FOUND)
	}
}

func cmdPeekBuried(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdPeekBuried, 1)
	cmd.peekByState(jobBuriedState)
}

func cmdPeekDelayed(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdPeekDelayed, 1)
	cmd.peekByState(jobDelayedState)
}

func cmdPeekReady(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdPeekReady, 1)
	cmd.peekByState(jobReadyState)
}

func cmdPut(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdPut, 1)
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

func cmdQuit(cmd *cmd) {
	cmd.assertNumberOfArguments(0)
	atomic.AddInt64(&cmd.server.stats.CmdQuit, 1)
	cmd.closeConn <- true
}

func reserveCommon(cmd *cmd) *jobReserveRequest {
	request := &jobReserveRequest{
		client:  cmd.client,
		success: make(chan *job),
		cancel:  make(chan bool, 1),
	}

	for _, watchedTube := range cmd.client.watchedTubes {
		go func(t *tube, r *jobReserveRequest) {
			select {
			case t.jobDemand <- r:
			case <-r.cancel:
				r.cancel <- true
			}
		}(watchedTube, request)
	}

	return request
}

func cmdReserve(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdReserve, 1)
	cmd.assertNumberOfArguments(0)

	cmd.client.isWorker = true
	request := reserveCommon(cmd)
	job := <-request.success
	request.cancel <- true
	cmd.respond(fmt.Sprintf(MSG_RESERVED, job.id, len(job.body), job.body))
}

func cmdReserveWithTimeout(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdReserveWithTimeout, 1)

	cmd.assertNumberOfArguments(1) // seconds
	seconds := cmd.getInt(0)
	if seconds < 0 {
		seconds = 0
	}

	cmd.client.isWorker = true
	request := reserveCommon(cmd)

	select {
	case job := <-request.success:
		cmd.respond(fmt.Sprintf(MSG_RESERVED, job.id, len(job.body), job.body))
		request.cancel <- true
	case <-time.After(time.Duration(seconds) * time.Second):
		cmd.respond(MSG_TIMED_OUT)
		request.cancel <- true
	}
}

func cmdStats(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdStats, 1)
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

func cmdStatsJob(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdStatsJob, 1)
	cmd.assertNumberOfArguments(1)

	jobId := jobId(cmd.getInt(0))

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
func cmdStatsTube(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdStatsTube, 1)
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
func cmdTouch(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdTouch, 1)
	cmd.assertNumberOfArguments(1)
	jobId := jobId(cmd.getInt(0))

	job, found := cmd.server.findJob(jobId)

	if !found {
		cmd.respond(MSG_NOT_FOUND)
		return
	}

	job.touch()
	cmd.respond(MSG_TOUCHED)
}

func cmdUse(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdUse, 1)
	cmd.assertNumberOfArguments(1)

	name := cmd.args[0]
	if !NAME_CHARS.MatchString(name) {
		cmd.respond(MSG_BAD_FORMAT)
	}

	cmd.client.usedTube = cmd.server.findOrCreateTube(name)
	cmd.respond(fmt.Sprintf("USING %s\r\n", name))
}

func cmdWatch(cmd *cmd) {
	atomic.AddInt64(&cmd.server.stats.CmdWatch, 1)
	cmd.assertNumberOfArguments(1)

	name := cmd.args[0]
	if !NAME_CHARS.MatchString(name) {
		cmd.respond(MSG_BAD_FORMAT)
	}

	cmd.client.watchTube(name)
	cmd.respond("OK\r\n")
}
