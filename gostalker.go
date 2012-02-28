package gostalker

import (
  "log"
  "net"
  "os"
)

const (
  JOB_DATA_SIZE_LIMIT = (1 << 16) - 1

  FOUND           = "FOUND\r\n"
  NOTFOUND        = "NOT_FOUND\r\n"
  RESERVED        = "RESERVED\r\n"
  DEADLINE_SOON   = "DEADLINE_SOON\r\n"
  TIMED_OUT       = "TIMED_OUT\r\n"
  DELETED         = "DELETED\r\n"
  RELEASED        = "RELEASED\r\n"
  BURIED          = "BURIED\r\n"
  TOUCHED         = "TOUCHED\r\n"
  BURIED_FMT      = "BURIED %d\r\n"
  INSERTED_FMT    = "INSERTED %d\r\n"
  NOT_IGNORED     = "NOT_IGNORED\r\n"
  OUT_OF_MEMORY   = "OUT_OF_MEMORY\r\n"
  INTERNAL_ERROR  = "INTERNAL_ERROR\r\n"
  DRAINING        = "DRAINING\r\n"
  BAD_FORMAT      = "BAD_FORMAT\r\n"
  UNKNOWN_COMMAND = "UNKNOWN_COMMAND\r\n"
  EXPECTED_CRLF   = "EXPECTED_CRLF\r\n"
  JOB_TOO_BIG     = "JOB_TOO_BIG\r\n"
)

type Logger interface {
  Println(v ...interface{})
  Printf(string, ...interface{})
}

type Client struct {
  conn   net.Conn
  reader Reader
}

type Reader interface {
  ReadLine() ([]byte, bool, error)
  Read([]byte) (int, error)
}

type Response string

func Start() {
  logger := log.New(os.Stdout, "server: ", log.LstdFlags)
  server := &Server{logger, make(chan uint64, 42)}
  go server.runGetJobId()

  addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:40400")
  server.exitOn("net.ResolveTCPAddr", err)

  listener, err := net.ListenTCP("tcp", addr)
  server.exitOn("net.ListenTCP", err)

  for {
    conn, err := listener.AcceptTCP()
    server.exitOn("listener.AcceptTCP", err)
    server.log("Accepted Connection:", conn)
    err = conn.SetKeepAlive(true)
    server.exitOn("conn.SetKeepAlive", err)
    go server.accept(conn)
  }
}
