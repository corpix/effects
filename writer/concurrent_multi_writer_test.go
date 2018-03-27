package writer

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/corpix/pool"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
)

var (
	ConcurrentMultiWriterTestConfig = ConcurrentMultiWriterConfig{
		Backlog: 8, // How many writers could wait for free slots
		Pool: pool.Config{
			Workers:   1024, // How many writers we could serve
			QueueSize: 8,    // How many writes we could queue for each writer
		},
	}
	ConcurrentMultiWriterBenchConfig = ConcurrentMultiWriterConfig{
		Backlog: 64, // How many writers could wait for free slots
		Pool: pool.Config{
			Workers:   12800, // How many writers we could serve
			QueueSize: 1024,  // How many writes we could queue for each writer
		},
	}
)

type failingWriter struct {
	wrote int
	err   error
}

func (w failingWriter) Write([]byte) (int, error) {
	return w.wrote, w.err
}

func replicateBuffers(n uint) []io.Writer {
	var (
		res = make([]io.Writer, n)
	)
	for k := uint(0); k < n; k++ {
		res[k] = bytes.NewBuffer(nil)
	}

	return res
}

func replicateDiscard(n uint) []io.Writer {
	var (
		res = make([]io.Writer, n)
	)
	for k := uint(0); k < n; k++ {
		res[k] = ioutil.Discard
	}

	return res
}

func replicateByteslice(n uint, buf []byte) [][]byte {
	var (
		res = make([][]byte, n)
	)
	for k := uint(0); k < n; k++ {
		res[k] = buf
	}

	return res
}

type concurrentMultiWriterSamples []struct {
	name    string
	data    [][]byte
	buffers []io.Writer
}

func smallSeriesSamples() concurrentMultiWriterSamples {
	return concurrentMultiWriterSamples{
		{
			name:    "0:0",
			data:    replicateByteslice(0, []byte(tearsInRain)),
			buffers: replicateBuffers(0),
		},
		{
			name:    "0:1",
			data:    replicateByteslice(0, []byte(tearsInRain)),
			buffers: replicateBuffers(1),
		},
		{
			name:    "1:0",
			data:    replicateByteslice(1, []byte(tearsInRain)),
			buffers: replicateBuffers(0),
		},
		{
			name:    "1:1",
			data:    replicateByteslice(1, []byte(tearsInRain)),
			buffers: replicateBuffers(1),
		},
		{
			name:    "1:2",
			data:    replicateByteslice(1, []byte(tearsInRain)),
			buffers: replicateBuffers(2),
		},
		{
			name:    "2:1",
			data:    replicateByteslice(2, []byte(tearsInRain)),
			buffers: replicateBuffers(1),
		},
		{
			name:    "2:2",
			data:    replicateByteslice(2, []byte(tearsInRain)),
			buffers: replicateBuffers(2),
		},
		{
			name:    "4:4",
			data:    replicateByteslice(4, []byte(tearsInRain)),
			buffers: replicateBuffers(4),
		},
		{
			// FIXME: Big numbers leading to a deadlock
			name:    "100:512",
			data:    replicateByteslice(100, []byte(tearsInRain)),
			buffers: replicateBuffers(512),
		},
	}
}

func TestConcurrentMultiWriterWrite(t *testing.T) {
	var (
		samples = smallSeriesSamples()
	)

	for _, sample := range samples {
		t.Run(
			sample.name,
			func(t *testing.T) {
				w := NewConcurrentMultiWriter(
					ConcurrentMultiWriterTestConfig,
					func(w *ConcurrentMultiWriter, v io.Writer, err error) { panic(err) },
					sample.buffers...,
				)
				defer w.Close()

				for k, v := range sample.data {
					r := bytes.Join(sample.data[:k+1], nil)
					n, err := w.Write(v)

					assert.Equal(t, nil, err, sample.name)
					assert.Equal(t, len(v), n, sample.name)
					assert.Equal(t, 0, len(w.preemption))

					for k, buf := range sample.buffers {
						assert.Equal(
							t,
							r,
							buf.(*bytes.Buffer).Bytes(),
							fmt.Sprintf("Buffer index is %d", k),
						)
					}
				}
			},
		)
	}
}

func TestConcurrentMultiWriterAddWrite(t *testing.T) {
	var (
		samples = smallSeriesSamples()
	)

	for _, sample := range samples {
		t.Run(
			sample.name,
			func(t *testing.T) {
				w := NewConcurrentMultiWriter(
					ConcurrentMultiWriterTestConfig,
					func(w *ConcurrentMultiWriter, v io.Writer, err error) { panic(err) },
				)
				defer w.Close()

				for _, buf := range sample.buffers {
					if !w.TryAdd(buf) {
						panic("No available workers")
					}
				}

				for k, v := range sample.data {
					r := bytes.Join(sample.data[:k+1], nil)
					n, err := w.Write(v)

					assert.Equal(t, nil, err)
					assert.Equal(t, len(v), n)
					assert.Equal(t, 0, len(w.preemption))

					for _, buf := range sample.buffers {
						assert.Equal(
							t,
							r,
							buf.(*bytes.Buffer).Bytes(),
						)
					}
				}
			},
		)
	}
}

func TestConcurrentMultiWriterWriteRemoveWrite(t *testing.T) {
	var (
		samples = smallSeriesSamples()
	)

	for _, sample := range samples {
		t.Run(
			sample.name,
			func(t *testing.T) {
				w := NewConcurrentMultiWriter(
					ConcurrentMultiWriterTestConfig,
					func(w *ConcurrentMultiWriter, v io.Writer, err error) { panic(err) },
					sample.buffers...,
				)
				defer w.Close()

				for k, v := range sample.data {
					r := bytes.Join(sample.data[:k+1], nil)
					n, err := w.Write(v)

					assert.Equal(t, nil, err)
					assert.Equal(t, len(v), n)

					for _, buf := range sample.buffers {
						assert.Equal(
							t,
							r,
							buf.(*bytes.Buffer).Bytes(),
						)
					}

				}

				for _, buf := range sample.buffers {
					assert.Equal(
						t,
						true,
						w.Remove(buf),
					)
				}
				assert.Equal(t, 0, len(w.preemption))

				for _, v := range sample.data {
					n, err := w.Write(v)
					assert.Equal(t, nil, err)
					assert.Equal(t, len(v), n)
					assert.Equal(t, 0, len(w.preemption))

					for _, buf := range sample.buffers {
						assert.Equal(
							t,
							false,
							w.Has(buf),
						)
					}

				}
			},
		)
	}
}

func TestConcurrentMultiWriterAddWriteRemoveWrite(t *testing.T) {
	var (
		samples = smallSeriesSamples()
	)

	for _, sample := range samples {
		t.Run(
			sample.name,
			func(t *testing.T) {
				w := NewConcurrentMultiWriter(
					ConcurrentMultiWriterTestConfig,
					func(w *ConcurrentMultiWriter, v io.Writer, err error) { panic(err) },
				)
				defer w.Close()

				for _, buf := range sample.buffers {
					if !w.TryAdd(buf) {
						panic("No available workers")
					}
				}

				for k, v := range sample.data {
					r := bytes.Join(sample.data[:k+1], nil)
					n, err := w.Write(v)

					assert.Equal(t, nil, err)
					assert.Equal(t, len(v), n)
					assert.Equal(t, 0, len(w.preemption))

					for _, buf := range sample.buffers {
						assert.Equal(
							t,
							r,
							buf.(*bytes.Buffer).Bytes(),
						)
					}
				}

				for _, buf := range sample.buffers {
					assert.Equal(
						t,
						true,
						w.Remove(buf),
					)
				}
				assert.Equal(t, 0, len(w.preemption))

				for _, v := range sample.data {
					n, err := w.Write(v)
					assert.Equal(t, nil, err)
					assert.Equal(t, len(v), n)

					for _, buf := range sample.buffers {
						assert.Equal(
							t,
							false,
							w.Has(buf),
						)
					}
				}
			},
		)
	}
}

func BenchmarkConcurrentMultiWriter(b *testing.B) {
	samples := []struct {
		name string
		w    io.WriteCloser
	}{
		{
			name: "none",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(w *ConcurrentMultiWriter, v io.Writer, err error) { panic(err) },
			),
		},
		{
			name: "1",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(w *ConcurrentMultiWriter, v io.Writer, err error) { panic(err) },
				replicateDiscard(1)...,
			),
		},
		{
			name: "2",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(w *ConcurrentMultiWriter, v io.Writer, err error) { panic(err) },
				replicateDiscard(2)...,
			),
		},
		{
			name: "3",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(w *ConcurrentMultiWriter, v io.Writer, err error) { panic(err) },
				replicateDiscard(3)...,
			),
		},
		{
			name: "512",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(w *ConcurrentMultiWriter, v io.Writer, err error) { panic(err) },
				replicateDiscard(512)...,
			),
		},
		{
			name: "2048",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(w *ConcurrentMultiWriter, v io.Writer, err error) { panic(err) },
				replicateDiscard(10000)...,
			),
		},
	}

	for _, sample := range samples {
		b.Run(
			sample.name,
			func(b *testing.B) {
				// run the Fib function b.N times
				for n := 0; n < b.N; n++ {
					_, err := sample.w.Write([]byte(tearsInRain))
					if err != nil {
						b.Error(err)
					}
				}
			},
		)

		defer sample.w.Close()
	}
}
