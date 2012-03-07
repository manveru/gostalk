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
	GOSTALK_VERSION     = "gostalk 2012-02-28"
	JOB_DATA_SIZE_LIMIT = (1 << 16) - 1
	MSG_FOUND           = "FOUND\r\n"
	MSG_NOTFOUND        = "NOT_FOUND\r\n"
	MSG_DEADLINE_SOON   = "DEADLINE_SOON\r\n"
	MSG_TIMED_OUT       = "TIMED_OUT\r\n"
	MSG_DELETED         = "DELETED\r\n"
	MSG_PAUSED          = "PAUSED\r\n"
	MSG_RELEASED        = "RELEASED\r\n"
	MSG_TOUCHED         = "TOUCHED\r\n"
	MSG_NOT_FOUND       = "NOT_FOUND\r\n"
	MSG_BURIED          = "BURIED %d\r\n"
	MSG_INSERTED        = "INSERTED %d\r\n"
	MSG_NOT_IGNORED     = "NOT_IGNORED\r\n"
	MSG_OUT_OF_MEMORY   = "OUT_OF_MEMORY\r\n"
	MSG_INTERNAL_ERROR  = "INTERNAL_ERROR\r\n"
	MSG_RESERVED        = "RESERVED %d %d\r\n%s\r\n"
	MSG_PEEK_FOUND      = "FOUND %d %d\r\n%s\r\n"
	MSG_DRAINING        = "DRAINING\r\n"
	MSG_BAD_FORMAT      = "BAD_FORMAT\r\n"
	MSG_UNKNOWN_COMMAND = "UNKNOWN_COMMAND\r\n"
	MSG_EXPECTED_CRLF   = "EXPECTED_CRLF\r\n"
	MSG_JOB_TOO_BIG     = "JOB_TOO_BIG\r\n"
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
