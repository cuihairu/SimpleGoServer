package pkg

type Option func(opts Options) error

type Options interface {
	Apply(opts ...Option) error
}

func Apply(options Options, opts ...Option) error {
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return err
		}
	}
	return nil
}
