package gostalk

import (
	"fmt"
	"launchpad.net/goyaml"
	"reflect"
	"syscall"
	"time"
)

// adds support for serializing structs
func toYaml(obj interface{}) (yaml []byte, err error) {
	objValue := reflect.ValueOf(obj)
	objKind := objValue.Kind()

	if objKind == reflect.Struct {
		raw := map[string]interface{}{}
		objType := objValue.Type()
		fieldCount := objValue.NumField()
		for n := 0; n < fieldCount; n += 1 {
			fieldType := objType.Field(n)
			fieldValue := objValue.Field(n)
			if fieldType.Tag == "" {
				raw[fieldType.Name] = fieldValue.Interface()
			} else {
				raw[string(fieldType.Tag)] = fieldValue.Interface()
			}
		}

		yaml, err = goyaml.Marshal(raw)
	} else {
		yaml, err = goyaml.Marshal(obj)
	}
	return
}

type tubeStats struct {
	Name                string "name"
	TotalJobs           int    "total-jobs"
	CurrentWaiting      int    "current-waiting"
	CmdDelete           int    "cmd-delete"
	CmdPauseTube        int    "cmd-pause-tube"
	Pause               int    "pause"
	PauseTimeLeft       int    "pause-time-left"
	CurrentUrgentJobs   int    "current-jobs-urgent"
	CurrentJobsBuried   int    "current-jobs-buried"
	CurrentJobsDelayed  int    "current-jobs-delayed"
	CurrentJobsReady    int    "current-jobs-ready"
	CurrentJobsReserved int    "current-jobs-reserved"
}

func (tube *Tube) statistics() tubeStats {
	stats := *(tube.stats)
	if tube.paused {
		stats.PauseTimeLeft = int(tube.pauseEndsAt.Sub(time.Now()).Seconds())
		stats.Pause = int(time.Since(tube.pauseStartedAt).Seconds())
	}
	return stats
}

type serverStats struct {
	BinlogCurrentIndex    int64   "binlog-current-index"    // TODO
	BinlogMaxSize         int64   "binlog-max-size"         // TODO
	BinlogOldestIndex     int64   "binlog-oldest-index"     // TODO
	BinlogRecordsMigrated int64   "binlog-records-migrated" // TODO
	BinlogRecordsWritten  int64   "binlog-records-written"  // TODO
	CmdBury               int64   "cmd-bury"
	CmdDelete             int64   "cmd-delete"
	CmdIgnore             int64   "cmd-ignore"
	CmdKick               int64   "cmd-kick"
	CmdListTubes          int64   "cmd-list-tubes"
	CmdListTubesWatched   int64   "cmd-list-tubes-watched"
	CmdListTubeUsed       int64   "cmd-list-tube-used"
	CmdPauseTube          int64   "cmd-pause-tube"
	CmdPeekBuried         int64   "cmd-peek-buried"
	CmdPeekDelayed        int64   "cmd-peek-delayed"
	CmdPeek               int64   "cmd-peek"
	CmdPeekReady          int64   "cmd-peek-ready"
	CmdPut                int64   "cmd-put"
	CmdQuit               int64   "cmd-quit"
	CmdReserve            int64   "cmd-reserve"
	CmdReserveWithTimeout int64   "cmd-reserve-with-timeout"
	CmdStats              int64   "cmd-stats"
	CmdStatsJob           int64   "cmd-stats-job"
	CmdStatsTube          int64   "cmd-stats-tube"
	CmdTouch              int64   "cmd-touch"
	CmdUse                int64   "cmd-use"
	CmdWatch              int64   "cmd-watch"
	CurrentConnections    int64   "current-connections"
	CurrentJobsBuried     int     "current-jobs-buried"
	CurrentJobsDelayed    int     "current-jobs-delayed"
	CurrentJobsReady      int     "current-jobs-ready"
	CurrentJobsReserved   int     "current-jobs-reserved"
	CurrentJobsUrgent     int     "current-jobs-urgent"
	CurrentProducers      int     "current-producers" // TODO
	CurrentTubes          int     "current-tubes"
	CurrentWaiting        int     "current-waiting" // TODO
	CurrentWorkers        int     "current-workers" // TODO
	GoCurrentGoroutines   int     "current-goroutines"
	MaxJobSize            int     "max-job-size"
	PID                   int     "pid"
	RusageStime           float64 "rusage-stime"
	RusageUtime           float64 "rusage-utime"
	TotalConnections      int64   "total-connections"
	TotalJobs             int     "total-jobs"
	TotalJobTimeouts      int64   "job-timeouts" // TODO
	Uptime                float64 "uptime"
	Version               string  "version"
}

func (server *Server) statistics() serverStats {
	stats := *server.stats
	stats.Uptime = time.Since(server.startedAt).Seconds()
	stats.CurrentTubes = len(server.tubes)
	stats.TotalJobs = len(server.jobs)

	for _, tube := range server.tubes {
		stats.CurrentJobsBuried += tube.buried.Len()
		stats.CurrentJobsDelayed += tube.delayed.Len()
		stats.CurrentJobsReady += tube.ready.Len()
		stats.CurrentJobsReserved += tube.reserved.Len()
	}

	var duration time.Duration
	usage := new(syscall.Rusage)
	err := syscall.Getrusage(syscall.RUSAGE_SELF, usage)
	if err == nil {
		s, ns := usage.Utime.Unix()
		duration, err = time.ParseDuration(fmt.Sprintf("%d.%ds", s, ns))
		stats.RusageStime = duration.Seconds()

		s, ns = usage.Stime.Unix()
		duration, err = time.ParseDuration(fmt.Sprintf("%d.%ds", s, ns))
		stats.RusageUtime = duration.Seconds()
	} else {
		pf("failed to get rusage : %v", err)
	}

	return stats
}
