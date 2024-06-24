package reactor

type Options struct {
	Multicore  bool // 是否使用多核
	NumWorkers int
	Listener   string
	LockThread bool
}
