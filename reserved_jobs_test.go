package gostalk

import (
	. "github.com/manveru/gobdd"
)

func init() {
	defer PrintSpecReport()

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

		It("orders jobs by ttr left, lowest first", func() {
			a := newJob(1, 0, 0, 100, []byte("a"))
			b := newJob(2, 0, 0, 25, []byte("b"))
			c := newJob(3, 0, 0, 50, []byte("c"))
			jobs.putJob(c)
			jobs.putJob(a)
			jobs.putJob(b)
			Expect(jobs.getJob(), ToDeepEqual, b)
			Expect(jobs.getJob(), ToDeepEqual, c)
			Expect(jobs.getJob(), ToDeepEqual, a)
		})
	})
}
