package gostalk

import (
	"bufio"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/manveru/gobdd"
	"gopkg.in/yaml.v2"
)

type jobResponse struct {
	id   jobId
	body string
}

func sendCommand(conn conn, raw string) {
	n, err := fmt.Fprintf(conn, raw+"\r\n")
	Expect(err, ToBeNil)
	Expect(n, ToEqual, len(raw)+2)
}

func sendCommands(conn conn, commands []string) (responses []string) {
	for _, command := range commands {
		sendCommand(conn, command)
	}

	return
}

func readResponseWithBody(reader reader, body interface{}) {
	line := readLine(reader)
	bodyLen, err := strconv.ParseInt(string(line[3:]), 10, 64)
	Expect(err, ToBeNil)

	rawYaml := make([]byte, bodyLen+2)
	n, err := reader.Read(rawYaml)
	Expect(err, ToBeNil)
	Expect(int64(n), ToEqual, bodyLen+2)
	err = yaml.Unmarshal(rawYaml[:len(rawYaml)-1], body)
	Expect(err, ToBeNil)
}

func readReserveResponse(reader reader) jobResponse {
	line := readLine(reader)
	lineParts := strings.Split(string(line), " ")
	Expect(lineParts[0], ToEqual, "RESERVED")

	id, err := strconv.ParseInt(lineParts[1], 10, 64)
	Expect(err, ToBeNil)

	bodyLen, err := strconv.ParseInt(lineParts[2], 10, 64)
	Expect(err, ToBeNil)

	jobBody := make([]byte, bodyLen+2)
	n, err := reader.Read(jobBody)
	Expect(err, ToBeNil)
	Expect(int64(n), ToEqual, bodyLen+2)
	return jobResponse{jobId(id), string(jobBody[:bodyLen])}
}

func readResponseWithoutBody(reader reader) (line string) {
	return string(readLine(reader))
}

func readLine(reader reader) []byte {
	line, isPrefix, err := reader.ReadLine()
	Expect(err, ToBeNil)
	Expect(isPrefix, ToEqual, false)
	return line
}

func TestEverything(t *testing.T) {}

// sort both actual and expected and compare them with reflect.DeepEqual.
func WhenSortedToEqual(actual, expected []string) (string, bool) {
	sort.Strings(actual)
	sort.Strings(expected)

	if reflect.DeepEqual(actual, expected) {
		return "", true
	}
	return fmt.Sprintf("    expected: %#v\nto deeply be: %#v\n", expected, actual), false
}

func init() {
	defer PrintSpecReport()

	Describe("readyJobs", func() {
		jobs := newReadyJobs()
		job := newJob(1, 1, 1, 1, []byte("foobar"))

		It("stores jobs", func() {
			jobs.putJob(job)
			Expect(jobs.Len(), ToEqual, 1)
		})

		It("retrieves jobs", func() {
			Expect(jobs.getJob(), ToDeepEqual, job)
		})

		It("panics when no jobs are available", func() {
			Expect(func() { jobs.getJob() }, ToPanicWith, "runtime error: index out of range")
		})

		It("orders jobs by priority, lowest first", func() {
			a := newJob(1, 10, 0, 0, []byte("a"))
			b := newJob(2, 20, 0, 0, []byte("b"))
			c := newJob(3, 15, 0, 0, []byte("c"))
			jobs.putJob(c)
			jobs.putJob(a)
			jobs.putJob(b)
			Expect(jobs.getJob(), ToDeepEqual, a) // 10
			Expect(jobs.getJob(), ToDeepEqual, c) // 15
			Expect(jobs.getJob(), ToDeepEqual, b) // 20
		})
	})

	Describe("protocol", func() {
		running := make(chan bool)
		go Start("127.0.0.1:40401", running)
		<-running
		conn, err := net.DialTimeout("tcp", "127.0.0.1:40401", 1*time.Second)
		Expect(err, ToBeNil)
		reader := bufio.NewReader(conn)

		It("handles list-tube-used", func() {
			var tubes []string
			sendCommand(conn, "list-tubes")
			readResponseWithBody(reader, &tubes)
			Expect(tubes, ToDeepEqual, []string{"default"})
		})

		It("accepts a list-tubes-watched command", func() {
			var tubes []string
			sendCommand(conn, "list-tubes-watched")
			readResponseWithBody(reader, &tubes)
			Expect(tubes, ToDeepEqual, []string{"default"})
		})

		It("handles watch <tube>", func() {
			sendCommand(conn, "watch test-tube")
			res := readResponseWithoutBody(reader)
			Expect(res, ToEqual, "OK")

			var tubes []string
			sendCommand(conn, "list-tubes-watched")
			readResponseWithBody(reader, &tubes)
			Expect(tubes, WhenSortedToEqual, []string{"test-tube", "default"})
		})

		It("handles ignore <tube>", func() {
			sendCommand(conn, "ignore test-tube")
			res := readResponseWithoutBody(reader)
			Expect(res, ToEqual, "WATCHING 1")

			var tubes []string
			sendCommand(conn, "list-tubes-watched")
			readResponseWithBody(reader, &tubes)
			Expect(tubes, ToDeepEqual, []string{"default"})
		})

		It("handles use <tube>", func() {
			sendCommand(conn, "use test-tube")
			res := readResponseWithoutBody(reader)
			Expect(res, ToEqual, "USING test-tube")

			sendCommand(conn, "list-tube-used")
			res = readResponseWithoutBody(reader)
			Expect(res, ToDeepEqual, "USING test-tube")
		})

		It("handles put <pri> <delay> <ttr> <bytes>", func() {
			sendCommand(conn, "put 0 0 0 2\r\nhi")
			res := readResponseWithoutBody(reader)
			Expect(res, ToEqual, "INSERTED 0")
		})

		It("handles reserve", func() {
			sendCommand(conn, "watch test-tube")
			res := readResponseWithoutBody(reader)
			Expect(res, ToEqual, "OK")

			sendCommand(conn, "reserve")
			Expect(readReserveResponse(reader), ToDeepEqual, jobResponse{0, "hi"})
		})

		It("times out on reserve-with-timeout <seconds>", func() {
			ok := make(chan string)

			go func() {
				sendCommand(conn, "reserve-with-timeout 1")
				ok <- readResponseWithoutBody(reader)
			}()

			select {
			case <-time.After(2 * time.Second):
				panic("too late")
			case res := <-ok:
				Expect(res, ToEqual, "TIMED_OUT")
			}
		})

		It("reserves on reserve-with-timeout <seconds>", func() {
			ok := make(chan jobResponse)

			go func() {
				sendCommand(conn, "reserve-with-timeout 2")
				ok <- readReserveResponse(reader)
			}()

			go func() {
				altConn, err := net.DialTimeout("tcp", "127.0.0.1:40401", 1*time.Second)
				Expect(err, ToBeNil)
				sendCommand(altConn, "use test-tube\r\nput 0 0 0 3\r\nlol\r\nquit")
			}()

			select {
			case <-time.After(3 * time.Second):
				panic("too late")
			case res := <-ok:
				Expect(res, ToEqual, jobResponse{1, "lol"})
			}
		})

		Describe("bury <id> <pri>", func() {
			It("can't bury unreserved jobs", func() {
				altConn, err := net.DialTimeout("tcp", "127.0.0.1:40401", 1*time.Second)
				Expect(err, ToBeNil)
				sendCommand(altConn, "use test-tube\r\nput 0 0 0 3\r\nlol\r\nquit")
				altConn.Close()
			})
		})
	})
}
