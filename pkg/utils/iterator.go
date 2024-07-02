package utils

type Iterator[T any] interface {
	HasNext() bool
	Next() (T, error)
}
type RangeAble[T any] interface {
	Iterator[T]
	Len() int
	Less(i int, j int) bool
	Swap(i int, j int)
}
