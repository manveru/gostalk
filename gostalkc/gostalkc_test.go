package gostalkc

import (
  . "github.com/manveru/gobdd"
  "gostalk"
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
      Expect(tubes, ToDeepEqual, []string{"default", "testing"})
    })

    Describe("ListTubeUsed", func() {
      tube, err := i.ListTubeUsed()
      Expect(err, ToBeNil)
      Expect(tube, ToDeepEqual, "default")
    })
  })
}
