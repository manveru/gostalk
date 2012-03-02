package gostalkc

import (
  . "github.com/manveru/gobdd"
  "gostalk"
  "reflect"
  "fmt"
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
      Expect(i.Watch("testing"), ToBeNil)
    })
    
    Describe("ListTubes", func() {
      tubes, err := i.ListTubes()
      Expect(err, ToBeNil)
      Expect(tubes, WhenSortedToEqual, []string{"default", "testing"})
    })

    Describe("ListTubeUsed", func() {
      tube, err := i.ListTubeUsed()
      Expect(err, ToBeNil)
      Expect(tube, ToDeepEqual, "default")
    })

    Describe("ListTubesWatched", func() {
      tubes, err := i.ListTubesWatched()
      Expect(err, ToBeNil)
      Expect(tubes, WhenSortedToEqual, []string{"default", "testing"})
    })

    Describe("ListTubesWatched", func() {
      tubes, err := i.ListTubesWatched()
      Expect(err, ToBeNil)
      Expect(tubes, WhenSortedToEqual, []string{"default", "testing"})
    })

    Describe("ListTubesWatched", func() {
      tubes, err := i.ListTubesWatched()
      Expect(err, ToBeNil)
      Expect(tubes, WhenSortedToEqual, []string{"default", "testing"})
    })

    Describe("Put", func() {
      jobId, buried, err := i.Put(1, 0, 0, []byte("hi"))
      Expect(err, ToBeNil)
      Expect(jobId, ToEqual, uint64(0))
      Expect(buried, ToEqual, false)
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
