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
  Watch(tubeName string) error
  ListTubes() ([]string, error)
  ListTubeUsed() (string, error)
  ListTubesWatched() ([]string, error)
  Delete(jobId uint64) error
  Touch(jobId uint64) error
  StatsJob(jobId uint64) (map[string]interface{}, error)
  ReserveWithTimeout(int) (uint64, []byte, error)
  Put(uint32, uint64, uint64, []byte) (uint64, bool, error)
  Ignore(tubeName string) (uint64, error)
  Reserve() (uint64, []byte, error)
}

type instance struct {
  conn       net.Conn
  readWriter *bufio.ReadWriter
}

const (
  BURIED        = "BURIED"
  USING         = "USING"
  OK            = "OK"
  DELETED       = "DELETED"
  DRAINING      = "DRAINING"
  TIMED_OUT     = "TIMED_OUT"
  EXPECTED_CRLF = "EXPECTED_CRLF"
  INSERTED      = "INSERTED"
  JOB_TOO_BIG   = "JOB_TOO_BIG"
  NOT_FOUND     = "NOT_FOUND"
  NOT_IGNORED   = "NOT_IGNORED"
  RESERVED      = "RESERVED"
  TOUCHED       = "TOUCHED"
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
  msgReserveWithTimeout = "reserve-with-timeout %d\r\n"
  msgTouch              = "touch %d\r\n"
  msgWatch              = "watch %s\r\n"
  msgStatsJob           = "stats-job %d\r\n"
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

func (i *instance) Watch(tubeName string) (err error) {
  logger.Println("Watch", tubeName)

  words, err := i.wordsCmd(fmt.Sprintf(msgWatch, tubeName))
  if err != nil {
    return
  }

  switch words[0] {
  case OK:
  default:
    err = Exception(words[0])
  }

  return
}

func (i *instance) ListTubes() (tubes []string, err error) {
  logger.Println("ListTubes")
  err = i.yamlCmd(msgListTubes, &tubes)
  return
}

func (i *instance) ListTubesWatched() (tubeNames []string, err error) {
  logger.Println("ListTubesWatched")
  err = i.yamlCmd(msgListTubesWatched, &tubeNames)
  return
}

func (i *instance) ListTubeUsed() (tubeName string, err error) {
  logger.Println("ListTubeUsed")
  words, err := i.wordsCmd(msgListTubeUsed)
  if err != nil {
    return
  }

  switch words[0] {
  case USING:
    tubeName = words[1]
  default:
    err = Exception(words[0])
  }

  return
}

func (i *instance) Ignore(tubeName string) (tubesLeft uint64, err error) {
  logger.Println("ListTubesWatched")
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

func (i *instance) Reserve() (jobId uint64, data []byte, err error) {
  i.write(msgReserve)

  line, err := i.readLine()
  if err != nil {
    return
  }

  words := strings.Split(line, " ")

  switch words[0] {
  case RESERVED:
    jobId, err = strconv.ParseUint(words[1], 10, 64)
    if err != nil {
      return
    }

    var dataLen uint64
    dataLen, err = strconv.ParseUint(words[2], 10, 64)
    if err != nil {
      return
    }

    data = make([]byte, dataLen+2)
    var n int
    n, err = i.readWriter.Read(data)
    if err != nil {
      return
    }
    if n != len(data) {
      err = Exception(fmt.Sprintf("read only %d bytes of %d", n, len(data)))
      return
    }

    data = data[:len(data)-2]
  }

  return
}

func (i *instance) ReserveWithTimeout(timeout int) (jobId uint64, data []byte, err error) {
  words, err := i.wordsCmd(fmt.Sprintf(msgReserveWithTimeout, timeout))
  if err != nil {
    return
  }

  switch words[0] {
  case RESERVED:
    jobId, err = strconv.ParseUint(words[1], 10, 64)
    if err != nil {
      return
    }

    var dataLen uint64
    dataLen, err = strconv.ParseUint(words[2], 10, 64)
    if err != nil {
      return
    }

    data = make([]byte, dataLen+2)
    var n int
    n, err = i.readWriter.Read(data)
    if err != nil {
      return
    }
    if n != len(data) {
      err = Exception(fmt.Sprintf("read only %d bytes of %d", n, len(data)))
      return
    }

    data = data[:len(data)-2]
  default:
    err = Exception(words[0])
  }

  return
}

func (i *instance) Touch(jobId uint64) (err error) {
  i.write(fmt.Sprintf(msgTouch, jobId))

  line, err := i.readLine()
  if err != nil {
    return
  }

  switch line {
  case TOUCHED:
    return
  case NOT_FOUND:
    err = Exception(NOT_FOUND)
  }

  return
}

func (i *instance) Delete(jobId uint64) (err error) {
  i.write(fmt.Sprintf(msgDelete, jobId))

  line, err := i.readLine()
  if err != nil {
    return
  }

  if line == DELETED {
    return
  }

  return Exception(line)
}

func (i *instance) StatsJob(jobId uint64) (stats map[string]interface{}, err error) {
  err = i.yamlCmd(fmt.Sprintf(msgStatsJob, jobId), &stats)
  return
}

func (i *instance) write(line string) (err error) {
  logger.Printf("i.write %#v\n", line)
  n, err := i.readWriter.WriteString(line)
  i.readWriter.Flush()

  if err == nil && n != len(line) {
    err = Exception(fmt.Sprintf("wrote only %d bytes of %d", n, len(line)))
  }

  return
}

func (i *instance) readLine() (line string, err error) {
  logger.Println("i.readLine")
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
