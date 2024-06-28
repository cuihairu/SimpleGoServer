package pkg

type Action func()

type Executor interface {
	Start()
	Exec(action Action)
	Stop()
}
