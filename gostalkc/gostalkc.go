package gostalkc

import (
	"bufio"
	"bytes"
	"fmt"
	"launchpad.net/goyaml"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Instance interface {
	Delete(jobId uint64) (err error)
	Ignore(tubeName string) (tubesLeft uint64, err error)
	Bury(jobId uint64) (err error)
	Kick(bound int) (actuallyKicked uint64, err error)
	ListTubes() (tubeNames []string, err error)
	ListTubesWatched() (tubeNames []string, err error)
	ListTubeUsed() (tubeName string, err error)
	Put(priority uint32, delay uint64, ttr uint64, jobData []byte) (jobId uint64, buried bool, err error)
	Reserve() (jobId uint64, jobData []byte, err error)
	ReserveWithTimeout(seconds int) (jobId uint64, jobData []byte, err error)
	StatsJob(jobId uint64) (stats map[string]interface{}, err error)
	StatsTube(tubeName string) (stats map[string]interface{}, err error)
	Stats() (stats map[string]interface{}, err error)
	Touch(jobId uint64) (err error)
	Watch(tubeName string) (err error)
}

type instance struct {
	conn       net.Conn
	readWriter *bufio.ReadWriter
}

const (
	BURIED        = "BURIED"
	DELETED       = "DELETED"
	DRAINING      = "DRAINING"
	EXPECTED_CRLF = "EXPECTED_CRLF"
	KICKED        = "KICKED"
	INSERTED      = "INSERTED"
	JOB_TOO_BIG   = "JOB_TOO_BIG"
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
	msgDelete             = "delete %d\r\n"
	msgIgnore             = "ignore %s\r\n"
	msgListTubes          = "list-tubes\r\n"
	msgListTubesWatched   = "list-tubes-watched\r\n"
	msgListTubeUsed       = "list-tube-used\r\n"
	msgPut                = "put %d %d %d %d\r\n%s\r\n"
	msgReserve            = "reserve\r\n"
	msgKick               = "kick %d\r\n"
	msgReserveWithTimeout = "reserve-with-timeout %d\r\n"
	msgBury               = "bury %d\r\n"
	msgStatsJob           = "stats-job %d\r\n"
	msgStats              = "stats\r\n"
	msgStatsTube          = "stats-tube %s\r\n"
	msgTouch              = "touch %d\r\n"
	msgWatch              = "watch %s\r\n"
)

var (
	logger = log.New(os.Stdout, "stalkc ", log.LstdFlags)
)

type Exception string

func (e Exception) Error() string {
	return string(e)
}

func Dial(hostAndPort string) (i Instance, err error) {
	conn, err := net.Dial("tcp", hostAndPort)
	if err == nil {
		i = newInstance(conn)
	}
	return
}

func DialTimeout(hostAndPort string, timeout time.Duration) (i Instance, err error) {
	conn, err := net.DialTimeout("tcp", hostAndPort, timeout)
	if err == nil {
		i = newInstance(conn)
	}
	return
}

func newInstance(conn net.Conn) (i Instance) {
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	return &instance{
		conn:       conn,
		readWriter: bufio.NewReadWriter(r, w),
	}
}

func (i *instance) write(line string) (err error) {
	n, err := i.readWriter.WriteString(line)
	i.readWriter.Flush()

	if err == nil && n != len(line) {
		err = Exception(fmt.Sprintf("wrote only %d bytes of %d", n, len(line)))
	}

	return
}

func (i *instance) readLine() (line string, err error) {
	lineBuf := new(bytes.Buffer)
	var linePart []byte
	isPrefix := true

	for isPrefix {
		linePart, isPrefix, err = i.readWriter.ReadLine()
		if err != nil {
			return line, err
		}
		lineBuf.Write(linePart)
	}
	return lineBuf.String(), nil
}

func (i *instance) wordsCmd(command string) (words []string, err error) {
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

func (i *instance) yamlCmd(command string, dest interface{}) (err error) {
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
		return Exception(words[0])
	}

	bodyLen, err := strconv.ParseInt(words[1], 10, 64)
	if err != nil {
		return
	}

	rawYaml := make([]byte, bodyLen+2)
	n, err := i.readWriter.Read(rawYaml)
	if err != nil {
		return
	}
	if n != len(rawYaml) {
		err = Exception(fmt.Sprintf("read only %d bytes of %d", n, len(rawYaml)))
	}

	err = goyaml.Unmarshal(rawYaml[:len(rawYaml)-1], dest)
	return err
}

func (i *instance) readJob(args []string) (jobId uint64, jobData []byte, err error) {
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
	n, err = i.readWriter.Read(jobData)
	if err != nil {
		return
	}
	if n != len(jobData) {
		err = Exception(fmt.Sprintf("read only %d bytes of %d", n, len(jobData)))
		return
	}

	jobData = jobData[:len(jobData)-2]
	return
}

func (i *instance) Watch(tubeName string) (err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgWatch, tubeName))
	if err == nil {
		if words[0] != OK {
			err = Exception(words[0])
		}
	}

	return
}

func (i *instance) Bury(jobId uint64) (err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgBury, jobId))
	if err == nil {
		if words[0] != BURIED {
			err = Exception(words[0])
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
func (i *instance) Kick(bound int) (actuallyKicked uint64, err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgKick, bound))
	if err == nil {
		if words[0] == KICKED {
			actuallyKicked, err = strconv.ParseUint(words[1], 10, 64)
		} else {
			err = Exception(words[0])
		}
	}
	return
}

func (i *instance) ListTubes() (tubes []string, err error) {
	err = i.yamlCmd(msgListTubes, &tubes)
	return
}

func (i *instance) ListTubesWatched() (tubeNames []string, err error) {
	err = i.yamlCmd(msgListTubesWatched, &tubeNames)
	return
}

func (i *instance) ListTubeUsed() (tubeName string, err error) {
	words, err := i.wordsCmd(msgListTubeUsed)
	if err == nil {
		if words[0] == USING {
			tubeName = words[1]
		} else {
			err = Exception(words[0])
		}
	}

	return
}

func (i *instance) Ignore(tubeName string) (tubesLeft uint64, err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgIgnore, tubeName))
	if err != nil {
		return
	}

	switch words[0] {
	case WATCHING:
		return strconv.ParseUint(words[1], 10, 64)
	case NOT_IGNORED:
		err = Exception(NOT_IGNORED)
	default:
		err = Exception(words[0])
	}

	return
}

func (i *instance) Put(priority uint32, delay, ttr uint64, data []byte) (jobId uint64, buried bool, err error) {
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
		err = Exception(EXPECTED_CRLF)
	case JOB_TOO_BIG:
		err = Exception(JOB_TOO_BIG)
	case DRAINING:
		err = Exception(DRAINING)
	}

	return
}

func (i *instance) Reserve() (jobId uint64, jobData []byte, err error) {
	words, err := i.wordsCmd(msgReserve)
	if err != nil {
		return
	}

	switch words[0] {
	case RESERVED:
		jobId, jobData, err = i.readJob(words[1:3])
	default:
		err = Exception(words[0])
	}

	return
}

func (i *instance) ReserveWithTimeout(timeout int) (jobId uint64, jobData []byte, err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgReserveWithTimeout, timeout))
	if err != nil {
		return
	}

	switch words[0] {
	case RESERVED:
		jobId, jobData, err = i.readJob(words[1:len(words)])
	default:
		err = Exception(words[0])
	}

	return
}

func (i *instance) Touch(jobId uint64) (err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgTouch, jobId))
	if err == nil {
		if words[0] != TOUCHED {
			err = Exception(words[0])
		}
	}

	return
}

func (i *instance) Delete(jobId uint64) (err error) {
	words, err := i.wordsCmd(fmt.Sprintf(msgDelete, jobId))
	if err == nil {
		if words[0] != DELETED {
			err = Exception(words[0])
		}
	}
	return
}

func (i *instance) StatsJob(jobId uint64) (stats map[string]interface{}, err error) {
	err = i.yamlCmd(fmt.Sprintf(msgStatsJob, jobId), &stats)
	return
}

func (i *instance) StatsTube(tubeName string) (stats map[string]interface{}, err error) {
	err = i.yamlCmd(fmt.Sprintf(msgStatsTube, tubeName), &stats)
	return
}

func (i *instance) Stats() (stats map[string]interface{}, err error) {
	err = i.yamlCmd(msgStats, &stats)
	return
}
