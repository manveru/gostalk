package gostalkc

import (
  "fmt"
  . "github.com/manveru/gobdd"
  "gostalk"
  "reflect"
  "sort"
  "testing"
  "time"
)

func TestEverything(t *testing.T) {}

func init() {
  defer PrintSpecReport()

  running := make(chan bool)
  go gostalk.Start("127.0.0.1:40402", running)
  <-running

  Describe("gostalkc", func() {
    i, err := DialTimeout("127.0.0.1:40402", 1*time.Second)
    Expect(err, ToBeNil)

    Describe("Watch", func() {
      It("watches another tube", func() {
        Expect(i.Watch("testing"), ToBeNil)
      })
    })

    Describe("ListTubes", func() {
      It("lists all tubes on the server", func() {
        tubes, err := i.ListTubes()
        Expect(err, ToBeNil)
        Expect(tubes, WhenSortedToEqual, []string{"default", "testing"})
      })
    })

    Describe("ListTubeUsed", func() {
      It("answers the tube our jobs are being put into", func() {
        tube, err := i.ListTubeUsed()
        Expect(err, ToBeNil)
        Expect(tube, ToDeepEqual, "default")
      })
    })

    Describe("ListTubesWatched", func() {
      It("lists the tubes we can receive jobs from", func() {
        tubes, err := i.ListTubesWatched()
        Expect(err, ToBeNil)
        Expect(tubes, WhenSortedToEqual, []string{"default", "testing"})
      })
    })

    Describe("Ignore", func() {
      It("removes a tube from the watch list", func() {
        amount, err := i.Ignore("testing")
        Expect(err, ToBeNil)
        Expect(amount, ToEqual, uint64(1))

        tubes, err := i.ListTubesWatched()
        Expect(err, ToBeNil)
        Expect(tubes, WhenSortedToEqual, []string{"default"})
      })

      It("cannot remove the last tube from the watch list", func() {
        amount, err := i.Ignore("testing")
        Expect(err, ToEqual, exception{NOT_IGNORED})
        Expect(amount, ToEqual, uint64(0))

        tubes, err := i.ListTubesWatched()
        Expect(err, ToBeNil)
        Expect(tubes, WhenSortedToEqual, []string{"default"})
      })
    })

    Describe("Put", func() {
      It("successfully puts a new job", func() {
        jobId, buried, err := i.Put(1, 0, 0, []byte("hi"))
        Expect(err, ToBeNil)
        Expect(jobId, ToEqual, uint64(0))
        Expect(buried, ToEqual, false)
      })
    })

    Describe("Reserve", func() {
      It("receives a job", func() {
        id, data, err := i.Reserve()
        Expect(err, ToBeNil)
        Expect(id, ToEqual, uint64(0))
        Expect(string(data), ToEqual, "hi")
      })
    })

    Describe("Touch", func() {
      It("tells the server to give us more time", func() {
      })
    })
  })
}

// sort both actual and expected and compare them with reflect.DeepEqual.
func WhenSortedToEqual(actual, expected []string) (string, bool) {
  sort.Strings(actual)
  sort.Strings(expected)

  if reflect.DeepEqual(actual, expected) {
    return "", true
  }
  return fmt.Sprintf("    expected: %#v\nto deeply be: %#v\n", expected, actual), false
}
