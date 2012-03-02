package gostalkc

import (
  . "github.com/manveru/gobdd"
  "testing"
  "github.com/manveru/gostalk"
)

func TestEverything(t *testing.T) {}

func init() {
  defer PrintSpecReport()

  running := make(chan bool)
  gostalk.Start("localhost:40402", running)
  <-running

  Describe("gostalkc", func(){
    i, err := DialTimeout("localhost:40400", 1)
    Expect(err, ToBeNil)
    Describe("Watch", func(){
      Expect(i.Watch("testing"), ToBeNil)
    })
  })
}
