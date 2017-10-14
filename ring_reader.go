package effects

import (
	"io"
)

type RingReader struct {
	buf []byte
}

func (r *RingReader) Read(buf []byte) (int, error) {
	var (
		n           int
		copiedBytes int
	)

	for {
		n = copy(buf[copiedBytes:], r.buf)
		copiedBytes += n
		if n == 0 {
			break
		}
	}

	return copiedBytes, nil
}

func NewRingReader(buf []byte) io.Reader {
	return &RingReader{
		buf: buf,
	}
}
