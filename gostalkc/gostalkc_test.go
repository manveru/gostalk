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
        Expect(err, ToEqual, Exception(NOT_IGNORED))
        Expect(amount, ToEqual, uint64(0))

        tubes, err := i.ListTubesWatched()
        Expect(err, ToBeNil)
        Expect(tubes, WhenSortedToEqual, []string{"default"})
      })
    })

    Describe("StatsTube", func() {
      It("returns stats about a given tube", func() {
        stats, err := i.StatsTube("testing")
        Expect(err, ToBeNil)
        Expect(stats["name"], ToEqual, "testing")
        Expect(stats["current-jobs-urgent"], ToEqual, 0)
        Expect(stats["current-jobs-ready"], ToEqual, 0)
        Expect(stats["current-jobs-reserved"], ToEqual, 0) // TODO: do at least a rough delta compare
        Expect(stats["current-jobs-delayed"], ToEqual, 0)
        Expect(stats["current-jobs-buried"], ToEqual, 0)
        Expect(stats["total-jobs"], ToEqual, 0)
        Expect(stats["current-waiting"], ToEqual, 0)
        Expect(stats["cmd-delete"], ToEqual, 0)
        Expect(stats["cmd-pause-tube"], ToEqual, 0)
        Expect(stats["pause"], ToEqual, 0)
        Expect(stats["pause-time-left"], ToEqual, 0)
      })
    })

    Describe("Put", func() {
      It("successfully puts a new job", func() {
        jobId, buried, err := i.Put(1, 0, 0, []byte("hi"))
        Expect(err, ToBeNil)
        Expect(jobId, ToEqual, uint64(0))
        Expect(buried, ToEqual, false)

        i.Delete(jobId)
        Expect(err, ToBeNil)
      })
    })

    Describe("StatsJob", func() {
      jobId, buried, err := i.Put(42, 0, 3, []byte("hi"))
      Expect(err, ToBeNil)
      Expect(jobId, ToEqual, uint64(1))
      Expect(buried, ToEqual, false)

      It("provides information about a job", func() {
        stats, err := i.StatsJob(jobId)
        Expect(err, ToBeNil)
        Expect(stats["id"], ToEqual, 1)
        Expect(stats["tube"], ToEqual, "default")
        Expect(stats["state"], ToEqual, "ready")
        Expect(stats["pri"], ToEqual, 42)
        Expect(stats["age"], ToNotBeNil) // TODO: do at least a rough delta compare
        Expect(stats["time-left"], ToEqual, 0)
        Expect(stats["file"], ToEqual, 0)
        Expect(stats["reserves"], ToEqual, 0)
        Expect(stats["releases"], ToEqual, 0)
        Expect(stats["timeouts"], ToEqual, 0)
        Expect(stats["buries"], ToEqual, 0)
        Expect(stats["kicks"], ToEqual, 0)
      })

      i.Delete(jobId)
      Expect(err, ToBeNil)
    })

    Describe("Reserve", func() {
      It("receives a job", func() {
        jobId, _, err := i.Put(1, 0, 3, []byte("hi"))
        Expect(err, ToBeNil)

        id, data, err := i.Reserve()
        Expect(err, ToBeNil)
        Expect(id, ToEqual, jobId)
        Expect(string(data), ToEqual, "hi")

        err = i.Delete(jobId)
        Expect(err, ToBeNil)
      })
    })

    Describe("Delete", func() {
      It("deletes a job", func() {
        jobId, buried, err := i.Put(0, 0, 0, []byte("hi"))
        Expect(err, ToBeNil)
        Expect(buried, ToEqual, false)

        err = i.Delete(jobId)
        Expect(err, ToBeNil)

        It("cannot delete the same job twice", func() {
          err := i.Delete(jobId)
          Expect(err, ToEqual, Exception(NOT_FOUND))
        })
      })

      It("cannot reserve the job after deletion", func() {
        jobId, buried, err := i.Put(0, 0, 0, []byte("hi"))
        Expect(err, ToBeNil)
        Expect(buried, ToEqual, false)

        err = i.Delete(jobId)
        Expect(err, ToBeNil)

        _, _, err = i.ReserveWithTimeout(0)
        Expect(err, ToEqual, Exception(TIMED_OUT))
      })
    })

    Describe("Touch", func() {
      It("tells the server to give us more time", func() {
        jobId, buried, err := i.Put(0, 0, 2, []byte("hi"))
        Expect(err, ToBeNil)
        Expect(buried, ToEqual, false)

        err = i.Touch(jobId)
        Expect(err, ToBeNil)

        id, data, err := i.Reserve()
        Expect(err, ToBeNil)
        Expect(id, ToEqual, jobId)
        Expect(string(data), ToEqual, "hi")
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
