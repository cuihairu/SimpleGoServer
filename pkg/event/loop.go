package event

type Loop interface {
	Run()
	Stop() error
}
