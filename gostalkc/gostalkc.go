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
  Put(uint32, uint64, uint64, []byte) (uint64, bool, error)
  Ignore(tubeName string) (uint64, error)
  Reserve() (uint64, []byte, error)
}

type instance struct {
  conn       net.Conn
  readWriter *bufio.ReadWriter
}

const (
  EXPECTED_CRLF         = "EXPECTED_CRLF"
  JOB_TOO_BIG           = "JOB_TOO_BIG"
  DRAINING              = "DRAINING"
  INSERTED              = "INSERTED"
  DELETED               = "DELETED"
  BURIED                = "BURIED"
  TOUCHED               = "TOUCHED"
  NOT_FOUND             = "NOT_FOUND"
  RESERVED              = "RESERVED"
  NOT_IGNORED           = "NOT_IGNORED"
  WATCHING              = "WATCHING"
  msgPut                = "put %d %d %d %d\r\n%s\r\n"
  msgListTubesWatched   = "list-tubes-watched\r\n"
  msgDelete             = "delete %d\r\n"
  msgWatch              = "watch %s\r\n"
  msgListTubes          = "list-tubes\r\n"
  msgListTubeUsed       = "list-tube-used\r\n"
  msgReserve            = "reserve\r\n"
  msgReserveWithTimeout = "reserve-with-timeout %d\r\n"
  msgTouch              = "touch %d\r\n"
  msgIgnore             = "ignore %s\r\n"
)

var (
  logger = log.New(os.Stdout, "stalkc ", log.LstdFlags)
)

type exception struct {
  msg string
}

func (be exception) Error() string {
  return be.msg
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

  err = i.write(fmt.Sprintf(msgWatch, tubeName))
  if err != nil {
    return
  }

  line, err := i.readLine()

  if line != "OK" {
    err = exception{line}
  }

  return
}

func (i *instance) ListTubes() (tubes []string, err error) {
  logger.Println("ListTubes")

  err = i.write(msgListTubes)
  if err != nil {
    return
  }

  line, err := i.readLine()
  if err != nil {
    return
  }

  words := strings.Split(line, " ")

  if words[0] != "OK" {
    return nil, exception{words[0]}
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
    err = exception{fmt.Sprintf("read only %d bytes of %d", n, len(rawYaml))}
  }

  dest := make([]string, 0)
  err = goyaml.Unmarshal(rawYaml[:len(rawYaml)-1], &dest)
  return dest, err
}

func (i *instance) ListTubeUsed() (tubeName string, err error) {
  logger.Println("ListTubeUsed")

  err = i.write(msgListTubeUsed)
  if err != nil {
    return
  }

  line, err := i.readLine()
  if err != nil {
    return
  }

  words := strings.Split(line, " ")
  if words[0] == "USING" {
    tubeName = words[1]
  } else {
    err = exception{words[0]}
  }

  return
}

func (i *instance) Ignore(tubeName string) (tubesLeft uint64, err error) {
  logger.Println("ListTubesWatched")

  err = i.write(fmt.Sprintf(msgIgnore, tubeName))
  if err != nil {
    return
  }

  line, err := i.readLine()
  if err != nil {
    return
  }

  words := strings.Split(line, " ")

  switch words[0] {
  case WATCHING:
    return strconv.ParseUint(words[1], 10, 64)
  case NOT_IGNORED:
    err = exception{NOT_IGNORED}
  default:
    err = exception{line}
  }

  return
}

func (i *instance) ListTubesWatched() (tubeNames []string, err error) {
  logger.Println("ListTubesWatched")

  err = i.write(msgListTubesWatched)
  if err != nil {
    return
  }

  line, err := i.readLine()
  if err != nil {
    return
  }

  words := strings.Split(line, " ")

  if words[0] != "OK" {
    return nil, exception{words[0]}
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
    err = exception{fmt.Sprintf("read only %d bytes of %d", n, len(rawYaml))}
  }

  dest := make([]string, 0)
  err = goyaml.Unmarshal(rawYaml[:len(rawYaml)-1], &dest)
  return dest, err
}

func (i *instance) Put(priority uint32, delay, ttr uint64, data []byte) (jobId uint64, buried bool, err error) {
  i.write(fmt.Sprintf(msgPut, priority, delay, ttr, len(data), data))

  line, err := i.readLine()
  if err != nil {
    return
  }

  words := strings.Split(line, " ")

  switch words[0] {
  case INSERTED:
    jobId, err = strconv.ParseUint(words[1], 10, 64)
  case BURIED:
    jobId, err = strconv.ParseUint(words[1], 10, 64)
    buried = true
  case EXPECTED_CRLF:
    err = exception{EXPECTED_CRLF}
  case JOB_TOO_BIG:
    err = exception{JOB_TOO_BIG}
  case DRAINING:
    err = exception{DRAINING}
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
      err = exception{fmt.Sprintf("read only %d bytes of %d", n, len(data))}
      return
    }

    data = data[:len(data)-2]
  }

  return
}

/*
The "touch" command allows a worker to request more time to work on a job.
This is useful for jobs that potentially take a long time, but you still want
the benefits of a TTR pulling a job away from an unresponsive worker.  A worker
may periodically tell the server that it's still alive and processing a job
(e.g. it may do this on DEADLINE_SOON).

The touch command looks like this:

touch <id>\r\n

 - <id> is the ID of a job reserved by the current connection.

There are two possible responses:

 - "TOUCHED\r\n" to indicate success.

 - "NOT_FOUND\r\n" if the job does not exist or is not reserved by the client.
*/
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
    err = exception{NOT_FOUND}
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

  return exception{line}
}

func (i *instance) write(line string) (err error) {
  logger.Printf("i.write %#v\n", line)
  n, err := i.readWriter.WriteString(line)
  i.readWriter.Flush()

  if err == nil && n != len(line) {
    err = exception{fmt.Sprintf("wrote only %d bytes of %d", n, len(line))}
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
