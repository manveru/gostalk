package gostalkc

import (
  "launchpad.net/goyaml"
  "strings"
  "bytes"
  "bufio"
  "strconv"
  "fmt"
  "log"
  "net"
  "os"
  "time"
)

var (
  logger = log.New(os.Stdout, "gostalkc ", log.LstdFlags)
)

type bufferError struct {
  msg string
}

func (be bufferError) Error() string {
  return be.msg
}

type Instance interface {
  Watch(tubeName string) error
  ListTubes() ([]string, error)
  ListTubeUsed() (string, error)
}

type instance struct {
  conn       net.Conn
  readWriter *bufio.ReadWriter
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
  log.Println("Watch", tubeName)
  out := fmt.Sprintf("watch %s\r\n", tubeName)
  n, err := i.readWriter.WriteString(out)
  i.readWriter.Flush()
  if err != nil { return err }
  if n != len(out) {
    return bufferError{fmt.Sprintf("wrote only %d bytes of %d", n, len(out))}
  }

  line, err := i.readLine()

  if line != "OK" {
    err = bufferError{line}
  }

  return
}

func (i *instance) ListTubes() (tubes []string, err error) {
  log.Println("ListTubes")

  err = i.writeLine("list-tubes")
  if err != nil { return }

  line, err := i.readLine()
  if err != nil { return }

  bodyLen, err := strconv.ParseInt(string(line[3:]), 10, 64)
  if err != nil { return }

  rawYaml := make([]byte, bodyLen+2)
  n, err := i.readWriter.Read(rawYaml)
  if err != nil { return }
  if n != len(rawYaml) {
    err = bufferError{fmt.Sprintf("read only %d bytes of %d", n, len(rawYaml))}
  }

  dest := make([]string, 0)
  err = goyaml.Unmarshal(rawYaml[:len(rawYaml)-1], &dest)
  return dest, err
}

func (i *instance) ListTubeUsed() (tubeName string, err error) {
  log.Println("ListTubeUsed")

  err = i.writeLine("list-tube-used")
  if err != nil { return }

  line, err := i.readLine()
  if err != nil { return }

  words := strings.Split(line, " ")
  if words[0] == "USING" {
    tubeName = words[1]
  } else {
    err = bufferError{words[0]}
  }

  return
}

func (i *instance) writeLine(line string) (err error) {
  log.Println("writeLine", line)
  out := line + "\r\n"
  n, err := i.readWriter.WriteString(out)
  i.readWriter.Flush()

  if err == nil && n != len(out) {
    err = bufferError{fmt.Sprintf("wrote only %d bytes of %d", n, len(out))}
  }

  return
}

func (i *instance) readLine() (line string, err error) {
  log.Println("i.readLine")
  lineBuf := new(bytes.Buffer)
  var linePart []byte
  isPrefix := true

  for isPrefix {
    log.Println("isPrefix:", isPrefix)
    got, err := i.readWriter.Peek(1)
    log.Println(got, err)
    linePart, isPrefix, err = i.readWriter.ReadLine()
    log.Println(linePart, isPrefix, err)
    if err != nil { return line, err }
    lineBuf.Write(linePart)
  }
  return lineBuf.String(), nil
}
