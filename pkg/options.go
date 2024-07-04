package pkg

type Options interface {
}

type Option[T Options] func(opts T) error

func Apply[T Options](options T, opts ...Option[T]) error {
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return err
		}
	}
	return nil
}

func With[T Options](opts ...Option[T]) Option[T] {
	return func(options T) error {
		return Apply(options, opts...)
	}
}
