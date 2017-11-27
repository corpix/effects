package closer

import (
	"io"
)

type Closers []io.Closer

func (c Closers) Close() error {
	var (
		err error
	)

	for _, closer := range c {
		err = closer.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
