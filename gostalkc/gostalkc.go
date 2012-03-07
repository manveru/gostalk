package gostalkc

import (
	"bufio"
	"bytes"
	"fmt"
	"launchpad.net/goyaml"
	"net"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	Conn       net.Conn
	ReadWriter *bufio.ReadWriter
}

const (
	BURIED        = "BURIED"
	DELETED       = "DELETED"
	DRAINING      = "DRAINING"
	EXPECTED_CRLF = "EXPECTED_CRLF"
	INSERTED      = "INSERTED"
	JOB_TOO_BIG   = "JOB_TOO_BIG"
	KICKED        = "KICKED"
	NOT_FOUND     = "NOT_FOUND"
	NOT_IGNORED   = "NOT_IGNORED"
	OK            = "OK"
	RESERVED      = "RESERVED"
	TIMED_OUT     = "TIMED_OUT"
	TOUCHED       = "TOUCHED"
	USING         = "USING"
	WATCHING      = "WATCHING"
)

const (
	msgBury               = "bury %d\r\n"
	msgDelete             = "delete %d\r\n"
	msgIgnore             = "ignore %s\r\n"
	msgKick               = "kick %d\r\n"
	msgListTubes          = "list-tubes\r\n"
	msgListTubesWatched   = "list-tubes-watched\r\n"
	msgListTubeUsed       = "list-tube-used\r\n"
	msgPut                = "put %d %d %d %d\r\n%s\r\n"
	msgReserve            = "reserve\r\n"
	msgReserveWithTimeout = "reserve-with-timeout %d\r\n"
	msgStatsJob           = "stats-job %d\r\n"
	msgStats              = "stats\r\n"
	msgStatsTube          = "stats-tube %s\r\n"
	msgTouch              = "touch %d\r\n"
	msgWatch              = "watch %s\r\n"
)

type exception string

func (e exception) Error() string {
	return string(e)
}

func Dial(hostAndPort string) (i *Client, err error) {
	conn, err := net.Dial("tcp", hostAndPort)
	if err == nil {
		i = newClient(conn)
	}
	return
}

func DialTimeout(hostAndPort string, timeout time.Duration) (i *Client, err error) {
	conn, err := net.DialTimeout("tcp", hostAndPort, timeout)
	if err == nil {
		i = newClient(conn)
	}
	return
}

func newClient(conn net.Conn) (i *Client) {
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	return &Client{
		Conn:       conn,
		ReadWriter: bufio.NewReadWriter(r, w),
	}
}

func (i *Client) write(line string) (err error) {
	n, err := i.ReadWriter.WriteString(line)
	i.ReadWriter.Flush()

	if err == nil && n != len(line) {
		err = exception(fmt.Sprintf("wrote only %d bytes of %d", n, len(line)))
	}

	return
}

func (i *Client) readLine() (line string, err error) {
	lineBuf := new(bytes.Buffer)
	var linePart []byte
	isPrefix := true

	for isPrefix {
		linePart, isPrefix, err = i.ReadWriter.ReadLine()
		if err != nil {
			return line, err
		}
		lineBuf.Write(linePart)
	}
	return lineBuf.String(), nil
}

func (i *Client) wordsCmd(command string) (words []string, err error) {
	err = i.write(command)
	if err != nil {
		return
	}

	line, err := i.readLine()
	if err != nil {
		return
	}

	words = strings.Split(line, " ")

	return
}

func (i *Client) yamlCmd(command string, dest interface{}) (err error) {
	err = i.write(command)
	if err != nil {
		return
	}

	line, err := i.readLine()
	if err != nil {
		return
	}

	words := strings.Split(line, " ")

	if words[0] != "OK" {
		return exception(words[0])
	}

	bodyLen, err := strconv.ParseInt(words[1], 10, 64)
	if err != nil {
		return
	}

	rawYaml := make([]byte, bodyLen+2)
	n, err := i.ReadWriter.Read(rawYaml)
	if err != nil {
		return
	}
	if n != len(rawYaml) {
		err = exception(fmt.Sprintf("read only %d bytes of %d", n, len(rawYaml)))
	}

	err = goyaml.Unmarshal(rawYaml[:len(rawYaml)-1], dest)
	return err
}

func (i *Client) readJob(args []string) (jobId uint64, jobData []byte, err error) {
	jobId, err = strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return
	}

	var jobDataLen uint64
	jobDataLen, err = strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return
	}

	jobData = make([]byte, jobDataLen+2)
	var n int
	n, err = i.ReadWriter.Read(jobData)
	if err != nil {
		return
	}
	if n != len(jobData) {
		err = exception(fmt.Sprintf("read only %d bytes of %d", n, len(jobData)))
		return
	}

	jobData = jobData[:len(jobData)-2]
	return
}

func (i *Client) Watch(tubeName string) (err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgWatch, tubeName))
	if err == nil {
		if words[0] != OK {
			err = exception(words[0])
		}
	}

	return
}

func (i *Client) Bury(jobId uint64) (err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgBury, jobId))
	if err == nil {
		if words[0] != BURIED {
			err = exception(words[0])
		}
	}

	return
}

/*
The kick command applies only to the currently used tube.
It moves jobs into the ready queue.
If there are any buried jobs, it will only kick buried jobs.
Otherwise it will kick delayed jobs.

The bound argument indicates the maximum number of jobs to kick.
*/
func (i *Client) Kick(bound int) (actuallyKicked uint64, err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgKick, bound))
	if err == nil {
		if words[0] == KICKED {
			actuallyKicked, err = strconv.ParseUint(words[1], 10, 64)
		} else {
			err = exception(words[0])
		}
	}
	return
}

func (i *Client) ListTubes() (tubes []string, err error) {
	err = i.yamlCmd(msgListTubes, &tubes)
	return
}

func (i *Client) ListTubesWatched() (tubeNames []string, err error) {
	err = i.yamlCmd(msgListTubesWatched, &tubeNames)
	return
}

func (i *Client) ListTubeUsed() (tubeName string, err error) {
	words, err := i.wordsCmd(msgListTubeUsed)
	if err == nil {
		if words[0] == USING {
			tubeName = words[1]
		} else {
			err = exception(words[0])
		}
	}

	return
}

func (i *Client) Ignore(tubeName string) (tubesLeft uint64, err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgIgnore, tubeName))
	if err != nil {
		return
	}

	switch words[0] {
	case WATCHING:
		return strconv.ParseUint(words[1], 10, 64)
	case NOT_IGNORED:
		err = exception(NOT_IGNORED)
	default:
		err = exception(words[0])
	}

	return
}

func (i *Client) Put(priority uint32, delay, ttr uint64, data []byte) (jobId uint64, buried bool, err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgPut, priority, delay, ttr, len(data), data))
	if err != nil {
		return
	}

	switch words[0] {
	case INSERTED:
		jobId, err = strconv.ParseUint(words[1], 10, 64)
	case BURIED:
		jobId, err = strconv.ParseUint(words[1], 10, 64)
		buried = true
	case EXPECTED_CRLF:
		err = exception(EXPECTED_CRLF)
	case JOB_TOO_BIG:
		err = exception(JOB_TOO_BIG)
	case DRAINING:
		err = exception(DRAINING)
	}

	return
}

func (i *Client) Reserve() (jobId uint64, jobData []byte, err error) {
	words, err := i.wordsCmd(msgReserve)
	if err != nil {
		return
	}

	switch words[0] {
	case RESERVED:
		jobId, jobData, err = i.readJob(words[1:3])
	default:
		err = exception(words[0])
	}

	return
}

func (i *Client) ReserveWithTimeout(timeout int) (jobId uint64, jobData []byte, err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgReserveWithTimeout, timeout))
	if err != nil {
		return
	}

	switch words[0] {
	case RESERVED:
		jobId, jobData, err = i.readJob(words[1:len(words)])
	default:
		err = exception(words[0])
	}

	return
}

func (i *Client) Touch(jobId uint64) (err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgTouch, jobId))
	if err == nil {
		if words[0] != TOUCHED {
			err = exception(words[0])
		}
	}

	return
}

func (i *Client) Delete(jobId uint64) (err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgDelete, jobId))
	if err == nil {
		if words[0] != DELETED {
			err = exception(words[0])
		}
	}
	return
}

func (i *Client) StatsJob(jobId uint64) (stats map[string]interface{}, err error) {
	err = i.yamlCmd(fmt.Sprintf(msgStatsJob, jobId), &stats)
	return
}

func (i *Client) StatsTube(tubeName string) (stats map[string]interface{}, err error) {
	err = i.yamlCmd(fmt.Sprintf(msgStatsTube, tubeName), &stats)
	return
}

func (i *Client) Stats() (stats map[string]interface{}, err error) {
	err = i.yamlCmd(msgStats, &stats)
	return
}
