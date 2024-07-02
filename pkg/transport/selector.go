package transport

type SelectorKeys uint32

const (
	SelectorKeyRead SelectorKeys = iota
	SelectorKeyWrite
)

type Selector interface {
	Select()
}
