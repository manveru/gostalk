package gostalk

type buriedJobs []*job

func newBuriedJobs() (jobs *buriedJobs) {
	return &buriedJobs{}
}

func (jobs *buriedJobs) putJob(job *job) {
	job.jobHolder = jobs
	*jobs = append(*jobs, job)
	job.index = len(*jobs)
}

func (jobs *buriedJobs) getJob() (job *job) {
	job = (*jobs)[0]
	*jobs = (*jobs)[1:len(*jobs)]
	return
}

func (jobs *buriedJobs) Len() int {
	return len(*jobs)
}

func (jobs *buriedJobs) buryJob(job *job) {
	// nothing to do here
}

func (jobs *buriedJobs) deleteJob(job *job) {
	for i, j := range *jobs {
		if j.id == job.id {
			*jobs = append((*jobs)[0:i], (*jobs)[i+1:]...)
			return
		}
	}
}

func (jobs *buriedJobs) touchJob(job *job) {
	// nothing to do
}

func (jobs *buriedJobs) kickJobs(bound int) (actual int) {
	for bound > 0 && len(*jobs) > 0 {
		job := jobs.getJob()
		job.tube.put(job)
		actual += 1
		bound -= 1
	}

	return
}

func (jobs *buriedJobs) peekJob(request *jobPeekRequest) {
	request.success <- (*jobs)[0]
}
