package gostalkc

import (
	"fmt"
	. "github.com/manveru/gobdd"
	"gostalk"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestEverything(t *testing.T) {}

func init() {
	defer PrintSpecReport()

	go func() {
		c := make(chan os.Signal)
		signal.Notify(c)
		for sig := range c {
			panic(sig)
		}
	}()

	running := make(chan bool)
	go gostalk.Start("127.0.0.1:40402", running)
	<-running

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
			Expect(err.Error(), ToEqual, NOT_IGNORED)
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
				Expect(err.Error(), ToEqual, NOT_FOUND)
			})
		})

		It("cannot reserve the job after deletion", func() {
			jobId, buried, err := i.Put(0, 0, 0, []byte("hi"))
			Expect(err, ToBeNil)
			Expect(buried, ToEqual, false)

			err = i.Delete(jobId)
			Expect(err, ToBeNil)

			_, _, err = i.ReserveWithTimeout(0)
			Expect(err.Error(), ToEqual, TIMED_OUT)
		})
	})

	Describe("Touch", func() {
		It("tells the server to give us more time", func() {
			jobId, _, err := i.Put(52, 0, 10, []byte("hi"))
			Expect(err, ToBeNil)

			id, data, err := i.Reserve()
			Expect(err, ToBeNil)
			Expect(id, ToEqual, jobId)
			Expect(string(data), ToEqual, "hi")

			// keep this whole until i add more tests for StatsJob
			stats, err := i.StatsJob(jobId)
			Expect(err, ToBeNil)
			Expect(stats["id"].(int), ToEqual, int(jobId))
			Expect(stats["tube"], ToEqual, "default")
			Expect(stats["state"], ToEqual, "reserved")
			Expect(stats["pri"], ToEqual, 52)
			Expect(stats["age"], ToNotBeNil) // TODO: do at least a rough delta compare
			Expect(stats["time-left"], ToBeFloatBetween, 9.99, 10.0)
			Expect(stats["file"], ToEqual, 0)
			Expect(stats["reserves"], ToEqual, 1)
			Expect(stats["releases"], ToEqual, 0)
			Expect(stats["timeouts"], ToEqual, 0)
			Expect(stats["buries"], ToEqual, 0)
			Expect(stats["kicks"], ToEqual, 0)

			time.Sleep(100 * time.Millisecond)

			stats, err = i.StatsJob(jobId)
			Expect(err, ToBeNil)
			Expect(stats["time-left"], ToBeFloatBetween, 9.89, 9.99)

			err = i.Touch(jobId)
			Expect(err, ToBeNil)

			stats, err = i.StatsJob(jobId)
			Expect(err, ToBeNil)
			Expect(stats["time-left"], ToBeFloatBetween, 9.99, 9.9999)

			err = i.Delete(jobId)
			Expect(err, ToBeNil)
		})
	})

	Describe("Bury", func() {
		It("buries a reserved job", func() {
			jobId, _, err := i.Put(52, 0, 10, []byte("hi"))
			Expect(err, ToBeNil)

			id, data, err := i.Reserve()
			Expect(err, ToBeNil)
			Expect(id, ToEqual, jobId)
			Expect(string(data), ToEqual, "hi")

			err = i.Bury(jobId)
			Expect(err, ToBeNil)

			stats, err := i.StatsJob(jobId)
			Expect(err, ToBeNil)
			Expect(stats["id"].(int), ToEqual, int(jobId))
			Expect(stats["tube"], ToEqual, "default")
			Expect(stats["state"], ToEqual, "buried")
			Expect(stats["pri"], ToEqual, 52)
			Expect(stats["age"], ToNotBeNil) // TODO: do at least a rough delta compare
			Expect(stats["time-left"], ToEqual, 0)
			Expect(stats["file"], ToEqual, 0)
			Expect(stats["reserves"], ToEqual, 1)
			Expect(stats["releases"], ToEqual, 0)
			Expect(stats["timeouts"], ToEqual, 0)
			Expect(stats["buries"], ToEqual, 1)
			Expect(stats["kicks"], ToEqual, 0)

			err = i.Delete(jobId)
			Expect(err, ToBeNil)
		})
	})

	Describe("Kick", func() {
		jobId, _, err := i.Put(52, 0, 10, []byte("hi"))
		Expect(err, ToBeNil)

		id, data, err := i.Reserve()
		Expect(err, ToBeNil)
		Expect(id, ToEqual, jobId)
		Expect(string(data), ToEqual, "hi")

		err = i.Bury(jobId)
		Expect(err, ToBeNil)

		kicked, err := i.Kick(1)
		Expect(err, ToBeNil)
		Expect(kicked, ToEqual, uint64(1))

		err = i.Delete(jobId)
		Expect(err, ToBeNil)
	})

	Describe("StatsTube", func() {
		It("returns stats about a given tube", func() {
			stats, err := i.StatsTube("default")
			Expect(err, ToBeNil)
			It("shows the name of the tube", func() {
				Expect(stats["name"], ToEqual, "default")
			})
			It("counts the amount of urgent jobs", func() {
				Expect(stats["current-jobs-urgent"], ToEqual, 0)
			})
			It("counts the amount of ready jobs", func() {
				Expect(stats["current-jobs-ready"], ToEqual, 0)
			})
			It("counts the amount of reserved jobs", func() {
				Expect(stats["current-jobs-reserved"], ToEqual, 0)
			})
			It("counts the amount of delayed jobs", func() {
				Expect(stats["current-jobs-delayed"], ToEqual, 0)
			})
			It("counts the amount of buried jobs", func() {
				Expect(stats["current-jobs-buried"], ToEqual, 0)
			})
			It("counts the amount of total jobs", func() {
				Expect(stats["total-jobs"], ToEqual, 0)
			})
			It("counts the amount of waiting jobs", func() {
				Expect(stats["current-waiting"], ToEqual, 0)
			})
			It("counts the number of times a job has been deleted", func() {
				Expect(stats["cmd-delete"], ToEqual, 0)
			})
			It("counts the number of times the tube has been paused", func() {
				Expect(stats["cmd-pause-tube"], ToEqual, 0)
			})

			Expect(stats["pause"], ToEqual, 0)
			Expect(stats["pause-time-left"], ToEqual, 0)
		})
	})

	Describe("Stats", func() {
		stats, err := i.Stats()
		Expect(err, ToBeNil)

		It("has version", func() {
			Expect(stats["version"], ToDeepEqual, "gostalk 2012-02-28")
		})
		It("has uptime", func() {
			Expect(stats["uptime"], ToBeFloatBetween, 0.0, 1.0)
		})
		It("has total job count", func() {
			Expect(stats["total-jobs"], ToEqual, 0)
		})
		It("has total connection count", func() {
			Expect(stats["total-connections"], ToEqual, 1)
		})
		It("has the rusage utime", func() {
			Expect(stats["rusage-utime"], ToBeFloatBetween, 0.0, 1.0)
		})
		It("has the rusage stime", func() {
			Expect(stats["rusage-stime"], ToBeFloatBetween, 0.0, 1.0)
		})
		It("has the pid of the server", func() {
			Expect(stats["pid"], ToEqual, os.Getpid())
		})
		It("has the maximum size of a job body", func() {
			Expect(stats["max-job-size"], ToEqual, 65535)
		})
		It("has the amount of ", func() {
			Expect(stats["binlog-current-index"], ToEqual, 0)
		})
		It("has the amount of ", func() {
			Expect(stats["binlog-max-size"], ToEqual, 0)
		})
		It("has the amount of ", func() {
			Expect(stats["binlog-oldest-index"], ToEqual, 0)
		})
		It("has the amount of ", func() {
			Expect(stats["binlog-records-migrated"], ToEqual, 0)
		})
		It("has the amount of ", func() {
			Expect(stats["binlog-records-written"], ToEqual, 0)
		})
		It("has the number of times the 'bury' command was called", func() {
			Expect(stats["cmd-bury"], ToEqual, 2)
		})
		It("has the number of times the 'delete' command was called ", func() {
			Expect(stats["cmd-delete"], ToEqual, 9)
		})
		It("has the number of times the 'ignore' command was called ", func() {
			Expect(stats["cmd-ignore"], ToEqual, 2)
		})
		It("has the number of times the 'kick' command was called ", func() {
			Expect(stats["cmd-kick"], ToEqual, 1)
		})
		It("has the number of times the 'list-tubes' command was called ", func() {
			Expect(stats["cmd-list-tubes"], ToEqual, 1)
		})
		It("has the number of times the 'list-tubes-watched' command was called ", func() {
			Expect(stats["cmd-list-tubes-watched"], ToEqual, 3)
		})
		It("has the number of times the 'list-tube-used' command was called ", func() {
			Expect(stats["cmd-list-tube-used"], ToEqual, 1)
		})
		It("has the number of times the 'pause-tube' command was called ", func() {
			Expect(stats["cmd-pause-tube"], ToEqual, 0)
		})
		It("has the number of times the 'peek-buried' command was called ", func() {
			Expect(stats["cmd-peek-buried"], ToEqual, 0)
		})
		It("has the number of times the 'peek-delayed' command was called ", func() {
			Expect(stats["cmd-peek-delayed"], ToEqual, 0)
		})
		It("has the number of times the 'peek' command was called ", func() {
			Expect(stats["cmd-peek"], ToEqual, 0)
		})
		It("has the number of times the 'peek-ready' command was called ", func() {
			Expect(stats["cmd-peek-ready"], ToEqual, 0)
		})
		It("has the number of times the 'put' command was called ", func() {
			Expect(stats["cmd-put"], ToEqual, 8)
		})
		It("has the number of times the 'quit' command was called ", func() {
			Expect(stats["cmd-quit"], ToEqual, 0)
		})
		It("has the number of times the 'reserve' command was called ", func() {
			Expect(stats["cmd-reserve"], ToEqual, 4)
		})
		It("has the number of times the 'reserve-with-timeout' command was called ", func() {
			Expect(stats["cmd-reserve-with-timeout"], ToEqual, 1)
		})
		It("has the number of times the 'stats' command was called ", func() {
			Expect(stats["cmd-stats"], ToEqual, 1)
		})
		It("has the number of times the 'stats-job' command was called ", func() {
			Expect(stats["cmd-stats-job"], ToEqual, 5)
		})
		It("has the number of times the 'stats-tube' command was called ", func() {
			Expect(stats["cmd-stats-tube"], ToEqual, 1)
		})
		It("has the number of times the 'touch' command was called ", func() {
			Expect(stats["cmd-touch"], ToEqual, 1)
		})
		It("has the number of times the 'use' command was called", func() {
			Expect(stats["cmd-use"], ToEqual, 0)
		})
		It("has the number of times the 'watch' command was called", func() {
			Expect(stats["cmd-watch"], ToEqual, 1)
		})
		It("has the amount of current connections", func() {
			Expect(stats["current-connections"], ToEqual, 1)
		})
		It("has the amount of currently buried jobs", func() {
			Expect(stats["current-jobs-buried"], ToEqual, 0)
		})
		It("has the amount of currently delayed jobs", func() {
			Expect(stats["current-jobs-delayed"], ToEqual, 0)
		})
		It("has the amount of currently ready jobs", func() {
			Expect(stats["current-jobs-ready"], ToEqual, 0)
		})
		It("has the amount of currently reserved jobs", func() {
			Expect(stats["current-jobs-reserved"], ToEqual, 0)
		})
		It("has the amount of currently urgent jobs", func() {
			Expect(stats["current-jobs-urgent"], ToEqual, 0)
		})
		It("has the amount of currently connected producers", func() {
			Expect(stats["current-producers"], ToEqual, 0)
		})
		It("has the amount of currently active tubes", func() {
			Expect(stats["current-tubes"], ToEqual, 2)
		})
		It("has the amount of currently waiting clients ", func() {
			Expect(stats["current-waiting"], ToEqual, 0)
		})
		It("has the amount of currently connected workers", func() {
			Expect(stats["current-workers"], ToEqual, 0)
		})
		It("has the amount of current goroutines", func() {
			Expect(stats["current-goroutines"], ToEqual, 0)
		})
	})
}

func ToBeFloatBetween(f interface{}, lower, upper float64) (string, bool) {
	var actual float64

	switch f.(type) {
	case float64:
		actual = f.(float64)
	case int:
		actual = float64(f.(int))
	}

	if actual >= lower && actual <= upper {
		return "", true
	}
	return fmt.Sprintf("    expected: %#v\nto be between %#v and %#v\n", actual, lower, upper), false
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
