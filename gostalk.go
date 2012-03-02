package gostalk

import (
  logging "log"
  "net"
  "os"
  "regexp"
)

var (
  NAME_CHARS = regexp.MustCompile("\\A[A-Za-z0-9()_$.;/+][A-Za-z0-9()_$.;/+-]{0,200}\\z")
  log        = logging.New(os.Stdout, "stalk: ", logging.LstdFlags)
)

const (
  GOSTALK_VERSION   = "gostalk 2012-02-28"
  JOB_DATA_SIZE_LIMIT = (1 << 16) - 1
  FOUND               = "FOUND\r\n"
  NOTFOUND            = "NOT_FOUND\r\n"
  RESERVED            = "RESERVED\r\n"
  DEADLINE_SOON       = "DEADLINE_SOON\r\n"
  TIMED_OUT           = "TIMED_OUT\r\n"
  DELETED             = "DELETED\r\n"
  RELEASED            = "RELEASED\r\n"
  BURIED              = "BURIED\r\n"
  TOUCHED             = "TOUCHED\r\n"
  BURIED_FMT          = "BURIED %d\r\n"
  INSERTED_FMT        = "INSERTED %d\r\n"
  NOT_IGNORED         = "NOT_IGNORED\r\n"
  OUT_OF_MEMORY       = "OUT_OF_MEMORY\r\n"
  INTERNAL_ERROR      = "INTERNAL_ERROR\r\n"
  DRAINING            = "DRAINING\r\n"
  BAD_FORMAT          = "BAD_FORMAT\r\n"
  UNKNOWN_COMMAND     = "UNKNOWN_COMMAND\r\n"
  EXPECTED_CRLF       = "EXPECTED_CRLF\r\n"
  JOB_TOO_BIG         = "JOB_TOO_BIG\r\n"
)

func p(v ...interface{}) {
  log.Println(v...)
}

func pf(format string, v ...interface{}) {
  log.Printf(format, v...)
}

type GostalkError struct {
  msg string
}

func (e GostalkError) Error() string {
  return e.msg
}

func newError(msg string) *GostalkError {
  return &GostalkError{msg: msg}
}

type Logger interface {
  Println(v ...interface{})
  Printf(string, ...interface{})
}

type Reader interface {
  ReadLine() ([]byte, bool, error)
  Read([]byte) (int, error)
}

func Start(hostAndPort string, running chan bool) {
  server := newServer()

  addr, err := net.ResolveTCPAddr("tcp", hostAndPort)
  if err != nil {
    log.Fatalln("net.ResolveTCPAddr", err)
  }

  listener, err := net.ListenTCP("tcp", addr)
  if err != nil {
    log.Fatalln("net.ListenTCP", err)
  }

  running <- true

  for {
    conn, err := listener.AcceptTCP()
    if err != nil {
      p("listener.AcceptTCP", err)
    } else {
      p("Accepted Connection:", conn)
      err = conn.SetKeepAlive(true)
      if err != nil {
        p("conn.SetKeepAlive", err)
      } else {
        go server.accept(conn)
      }
    }
  }
}
