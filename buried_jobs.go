package gostalk

/*
The bury command puts a job into the "buried" state. Buried jobs are put into a
FIFO linked list and will not be touched by the server again until a client
kicks them with the "kick" command.

The bury command looks like this:

bury <id> <pri>\r\n

 - <id> is the job id to release.

 - <pri> is a new priority to assign to the job.

There are two possible responses:

 - "BURIED\r\n" to indicate success.

 - "NOT_FOUND\r\n" if the job does not exist or is not reserved by the client.
*/

type buriedJobs []*Job

func newBuriedJobs() (jobs *buriedJobs) {
  return &buriedJobs{}
}

func (jobs *buriedJobs) putJob(job *Job) {
}

func (jobs *buriedJobs) getJob() {
}

func (jobs *buriedJobs) Len() int {
  return len(*jobs)
}
