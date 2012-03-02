package gostalkc

import (
  "log"
  "bufio"
  "net"
  "time"
  "os"
  "fmt"
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
}

type instance struct {
  conn net.Conn
  readWriter *bufio.ReadWriter
}

func Dial(hostAndPort string) (i Instance, err error) {
  conn, err := net.Dial("tcp", hostAndPort)
  if err != nil { return }
  return newInstance(conn), nil
}

func DialTimeout(hostAndPort string, timeout time.Duration) (i Instance, err error) {
  conn, err := net.DialTimeout("tcp", hostAndPort, timeout)
  if err != nil { return }
  return newInstance(conn), nil
}

func newInstance(conn net.Conn) (i Instance) {
  r := bufio.NewReader(conn)
  w := bufio.NewWriter(conn)
  return &instance{
    conn: conn,
    readWriter: bufio.NewReadWriter(r, w),
  }
}

func (i *instance) Watch(tubeName string) (err error) {
  out := fmt.Sprintf("%s\r\n")
  n, err := i.readWriter.WriteString(out)
  if err == nil {
    if n != len(out) {
      return bufferError{fmt.Sprintf("wrote only %d bytes of %d", n, len(out))}
    }
  }

  return err
}
