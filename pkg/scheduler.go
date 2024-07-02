package pkg

type Task interface {
	Execute()
}

type Scheduler interface {
	Submit(task Task)
}
