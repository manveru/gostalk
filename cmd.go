package gostalk

import (
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"
)

type args []string

func (args args) getInt(idx int) (output int64) {
	output, err := strconv.ParseInt(args[idx], 10, 64)
	if err != nil {
		pf("args.getInt(%#v) : %v", args[idx], err)
		panic(MSG_BAD_FORMAT)
	}
	return
}

func (args args) getUint(idx int) (output uint64) {
	output, err := strconv.ParseUint(args[idx], 10, 64)
	if err != nil {
		pf("args.getInt(%#v) : %v", args[idx], err)
		panic(MSG_BAD_FORMAT)
	}
	return
}

func (args args) getJobId(idx int) jobId {
	output, err := strconv.ParseUint(args[idx], 10, 64)
	if err != nil {
		pf("args.getInt(%#v) : %v", args[idx], err)
		panic(MSG_BAD_FORMAT)
	}
	return jobId(output)
}

func (args args) getName(idx int) string {
	name := args[idx]
	if !NAME_CHARS.MatchString(name) {
		panic(MSG_BAD_FORMAT)
	}

	return name
}

var (
	commands = map[string]func(*client, args) string{
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

func cmdBury(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdBury, 1)

	job, found := client.server.jobs[args.getJobId(0)]
	if found && job.state == jobReservedState && job.client == client {
		job.bury()
		return MSG_BURIED
	}

	return MSG_NOT_FOUND
}

func cmdDelete(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdDelete, 1)

	job, found := client.server.jobs[args.getJobId(0)]
	if !found {
		return MSG_NOT_FOUND
	}

	switch job.state {
	case jobReservedState:
		if job.client == client {
			job.deleteFrom(client.server)
			return MSG_DELETED
		}
	case jobBuriedState, jobDelayedState, jobReadyState:
		job.deleteFrom(client.server)
		return MSG_DELETED
	}

	return MSG_NOT_FOUND
}

func cmdIgnore(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdIgnore, 1)

	name := args.getName(0)

	ignored, totalTubes := client.ignoreTube(name)
	if ignored {
		return fmt.Sprintf("WATCHING %d\r\n", totalTubes)
	}
	return "NOT_IGNORED\r\n"
}

func cmdKick(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdKick, 1)
	bound := args.getUint(0)

	jobKick := &jobKickRequest{
		bound:   int(bound),
		success: make(chan int),
	}

	client.usedTube.jobKick <- jobKick
	actual := <-jobKick.success

	return fmt.Sprintf("KICKED %d\r\n", actual)
}

func cmdListTubes(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdListTubes, 1)

	list := make([]string, 0)
	for key := range client.server.tubes {
		list = append(list, key)
	}

	yaml, err := toYaml(list)
	if err != nil {
		p(err)
		return MSG_INTERNAL_ERROR
	}

	return fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml)
}

func cmdListTubesWatched(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdListTubesWatched, 1)

	list := make([]string, 0)
	for _, tube := range client.watchedTubes {
		list = append(list, tube.name)
	}

	yaml, err := toYaml(list)
	if err != nil {
		p(err)
		return MSG_INTERNAL_ERROR
	}

	return fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml)
}
func cmdListTubeUsed(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdListTubeUsed, 1)
	return fmt.Sprintf("USING %s\r\n", client.usedTube.name)
}

func cmdPauseTube(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdPauseTube, 1)

	tube, found := client.server.findTube(args.getName(0))
	if !found {
		return MSG_NOT_FOUND
	}

	delay := args.getInt(1)
	tube.tubePause <- time.Duration(delay) * time.Second
	return MSG_PAUSED
}

func peekByState(client *client, state string) string {
	request := &jobPeekRequest{
		state:   state,
		success: make(chan *job),
	}
	client.usedTube.jobPeek <- request
	job := <-request.success
	if job != nil {
		return fmt.Sprintf(MSG_PEEK_FOUND, job.id, len(job.body), job.body)
	}
	return MSG_NOT_FOUND
}

func cmdPeek(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdPeek, 1)
	job, found := client.server.findJob(args.getJobId(0))

	if found {
		return fmt.Sprintf(MSG_PEEK_FOUND, job.id, len(job.body), job.body)
	}
	return MSG_NOT_FOUND
}

func cmdPeekBuried(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdPeekBuried, 1)
	return peekByState(client, jobBuriedState)
}

func cmdPeekDelayed(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdPeekDelayed, 1)
	return peekByState(client, jobDelayedState)
}

func cmdPeekReady(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdPeekReady, 1)
	return peekByState(client, jobReadyState)
}

func cmdPut(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdPut, 1)

	priority := uint32(args.getInt(0))
	if priority < 0 {
		priority = 0
	} else if priority > 4294967295 {
		priority = 4294967295
	}
	delay := args.getInt(1)
	ttr := args.getInt(2)
	if ttr < 1 {
		ttr = 1
	}
	bodySize := args.getInt(3)

	if bodySize > JOB_DATA_SIZE_LIMIT {
		return MSG_JOB_TOO_BIG
	}

	body := make([]byte, bodySize)
	_, err := io.ReadFull(client.reader, body)
	if err != nil {
		pf("io.ReadFull : %#v", err)
		return MSG_INTERNAL_ERROR
	}
	rn := make([]byte, 2)
	_, err = io.ReadAtLeast(client.reader, rn, 2)
	if err != nil {
		if err.Error() == "ErrUnexpextedEOF" {
			return MSG_EXPECTED_CRLF
		} else {
			pf("io.ReadAtLeast : %#v", err)
			return MSG_INTERNAL_ERROR
		}
	}

	if rn[0] != '\r' || rn[1] != '\n' {
		return MSG_EXPECTED_CRLF
	}

	tube := client.usedTube

	id := <-client.server.getJobId
	job := newJob(id, priority, delay, ttr, body)

	tube.jobSupply <- job
	client.server.jobs[job.id] = job
	client.isProducer = true
	return fmt.Sprintf("INSERTED %d\r\n", job.id)
}

func cmdQuit(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdQuit, 1)
	client.conn.Close()
	return ""
}

func reserveCommon(c *client, args args) *jobReserveRequest {
	request := &jobReserveRequest{
		client:  c,
		success: make(chan *job),
		cancel:  make(chan bool, 1),
	}

	for _, watchedTube := range c.watchedTubes {
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

func cmdReserve(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdReserve, 1)

	client.isWorker = true
	request := reserveCommon(client, args)
	job := <-request.success
	request.cancel <- true
	return fmt.Sprintf(MSG_RESERVED, job.id, len(job.body), job.body)
}

func cmdReserveWithTimeout(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdReserveWithTimeout, 1)

	seconds := args.getInt(0)
	if seconds < 0 {
		seconds = 0
	}

	client.isWorker = true
	request := reserveCommon(client, args)

	select {
	case job := <-request.success:
		response = fmt.Sprintf(MSG_RESERVED, job.id, len(job.body), job.body)
		request.cancel <- true
	case <-time.After(time.Duration(seconds) * time.Second):
		response = MSG_TIMED_OUT
		request.cancel <- true
	}

	return response
}

func cmdStats(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdStats, 1)

	stats := client.server.statistics()
	p(stats)
	yaml, err := toYaml(stats)
	if err != nil {
		p(err)
		return MSG_INTERNAL_ERROR
	}

	return fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml)
}

func cmdStatsJob(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdStatsJob, 1)

	job, found := client.server.findJob(args.getJobId(0))

	if !found {
		return MSG_NOT_FOUND
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
		return MSG_INTERNAL_ERROR
	}

	return fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml)
}
func cmdStatsTube(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdStatsTube, 1)

	tube, found := client.server.findTube(args.getName(0))

	if !found {
		return MSG_NOT_FOUND
	}

	stats := tube.statistics()

	yaml, err := toYaml(stats)
	if err != nil {
		p(err)
		return MSG_INTERNAL_ERROR
	}

	return fmt.Sprintf("OK %d\r\n%s\r\n", len(yaml), yaml)
}
func cmdTouch(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdTouch, 1)

	job, found := client.server.findJob(args.getJobId(0))

	if !found {
		return MSG_NOT_FOUND
	}

	job.touch()
	return MSG_TOUCHED
}

func cmdUse(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdUse, 1)

	name := args.getName(0)

	client.usedTube = client.server.findOrCreateTube(name)
	return fmt.Sprintf("USING %s\r\n", name)
}

func cmdWatch(client *client, args args) (response string) {
	atomic.AddInt64(&client.server.stats.CmdWatch, 1)

	client.watchTube(args.getName(0))
	return "OK\r\n"
}
