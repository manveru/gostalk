package gostalker

import (
  . "github.com/manveru/gobdd"
)

func init() {
  Describe("reservedJobs", func() {
    jobs := newReservedJobs()
    job := newJob(1, 1, 1, 1, []byte("barfoo"))

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
  })
}
