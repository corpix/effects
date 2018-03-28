package writer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/corpix/pool"
	timeHelpers "github.com/corpix/time"
	"github.com/stretchr/testify/assert"
)

var (
	ConcurrentMultiWriterTestConfig = ConcurrentMultiWriterConfig{
		Backlog: BacklogConfig{Size: 0, AddTimeout: timeHelpers.Duration(10 * time.Millisecond)},
		Pool: pool.Config{
			Workers:   512,
			QueueSize: 0,
		},
	}
	ConcurrentMultiWriterBenchConfig = ConcurrentMultiWriterConfig{
		Backlog: BacklogConfig{Size: 64, AddTimeout: timeHelpers.Duration(10 * time.Millisecond)},
		Pool: pool.Config{
			Workers:   12048,
			QueueSize: 1030,
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

type slowWriter struct {
	sleep time.Duration
}

func (w slowWriter) Write(buf []byte) (int, error) {
	time.Sleep(w.sleep)
	return len(buf), nil
}

func replicateWriter(n uint, fn func() io.Writer) []io.Writer {
	var (
		res = make([]io.Writer, n)
	)
	for k := uint(0); k < n; k++ {
		res[k] = fn()
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

func newBuffer() io.Writer  { return bytes.NewBuffer(nil) }
func newDiscard() io.Writer { return ioutil.Discard }
func newFailing(wrote int, err error) func() io.Writer {
	return func() io.Writer { return &failingWriter{wrote, err} }
}
func newSlow(sleep time.Duration) func() io.Writer {
	return func() io.Writer { return &slowWriter{sleep} }
}

type concurrentMultiWriterSamples []struct {
	name    string
	data    [][]byte
	buffers []io.Writer
}

func smallSeriesSamples(replicator func() io.Writer) concurrentMultiWriterSamples {
	return concurrentMultiWriterSamples{
		{
			name:    "0:0",
			data:    replicateByteslice(0, []byte(tearsInRain)),
			buffers: replicateWriter(0, replicator),
		},
		{
			name:    "0:1",
			data:    replicateByteslice(0, []byte(tearsInRain)),
			buffers: replicateWriter(1, replicator),
		},
		{
			name:    "1:0",
			data:    replicateByteslice(1, []byte(tearsInRain)),
			buffers: replicateWriter(0, replicator),
		},
		{
			name:    "1:1",
			data:    replicateByteslice(1, []byte(tearsInRain)),
			buffers: replicateWriter(1, replicator),
		},
		{
			name:    "1:2",
			data:    replicateByteslice(1, []byte(tearsInRain)),
			buffers: replicateWriter(2, replicator),
		},
		{
			name:    "2:1",
			data:    replicateByteslice(2, []byte(tearsInRain)),
			buffers: replicateWriter(1, replicator),
		},
		{
			name:    "2:2",
			data:    replicateByteslice(2, []byte(tearsInRain)),
			buffers: replicateWriter(2, replicator),
		},
		{
			name:    "4:4",
			data:    replicateByteslice(4, []byte(tearsInRain)),
			buffers: replicateWriter(4, replicator),
		},
		{
			name:    "10:512",
			data:    replicateByteslice(10, []byte(tearsInRain)),
			buffers: replicateWriter(512, replicator),
		},
	}
}

type benchmarkingMultiWriterSeriesSamples []struct {
	name string
	w    io.WriteCloser
}

func benchmarkingSeriesSamples(replicator func() io.Writer) benchmarkingMultiWriterSeriesSamples {
	return benchmarkingMultiWriterSeriesSamples{
		{
			name: "none",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
			),
		},
		{
			name: "1",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
				replicateWriter(1, newDiscard)...,
			),
		},
		{
			name: "2",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
				replicateWriter(2, newDiscard)...,
			),
		},
		{
			name: "3",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
				replicateWriter(3, newDiscard)...,
			),
		},
		{
			name: "512",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
				replicateWriter(512, newDiscard)...,
			),
		},
		{
			name: "2048",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
				replicateWriter(2048, newDiscard)...,
			),
		},
		{
			name: "3072",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
				replicateWriter(3072, newDiscard)...,
			),
		},
		{
			name: "3072:1 slow",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
				append(
					replicateWriter(3071, newDiscard),
					replicateWriter(1, newSlow(500*time.Millisecond))...,
				)...,
			),
		},
		{
			name: "3072:2 slow",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
				append(
					replicateWriter(3070, newDiscard),
					replicateWriter(2, newSlow(500*time.Millisecond))...,
				)...,
			),
		},
		{
			name: "3072:3 slow",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
				append(
					replicateWriter(3069, newDiscard),
					replicateWriter(3, newSlow(500*time.Millisecond))...,
				)...,
			),
		},
		{
			name: "3072:512 slow",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
				append(
					replicateWriter(2560, newDiscard),
					replicateWriter(512, newSlow(500*time.Millisecond))...,
				)...,
			),
		},
		{
			name: "3072:2048 slow",
			w: NewConcurrentMultiWriter(
				ConcurrentMultiWriterBenchConfig,
				func(err error) { panic(err) },
				append(
					replicateWriter(1024, newDiscard),
					replicateWriter(2048, newSlow(500*time.Millisecond))...,
				)...,
			),
		},
	}
}

func TestConcurrentMultiWriterWrite(t *testing.T) {
	var (
		samples = smallSeriesSamples(newBuffer)
	)

	for _, sample := range samples {
		t.Run(
			sample.name,
			func(t *testing.T) {
				w := NewConcurrentMultiWriter(
					ConcurrentMultiWriterTestConfig,
					func(err error) { panic(err) },
					sample.buffers...,
				)
				defer w.Close()

				for k, v := range sample.data {
					r := bytes.Join(sample.data[:k+1], nil)
					n, err := w.Write(v)

					assert.Equal(t, nil, err, sample.name)
					assert.Equal(t, len(v), n, sample.name)
					assert.Equal(t, 0, len(w.preempt))

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

func TestConcurrentMultiWriterWriteErrors(t *testing.T) {
	for _, errs := range []struct {
		err     error
		samples concurrentMultiWriterSamples
	}{
		{
			err:     NewErrWriter(errors.New("test"), nil),
			samples: smallSeriesSamples(newFailing(0, errors.New("test"))),
		},
		{
			err: NewErrBacklogOverflow(0, nil),
			samples: concurrentMultiWriterSamples{
				// FIXME: Test with small values
				{
					name:    "2:2",
					data:    replicateByteslice(2, []byte(tearsInRain)),
					buffers: replicateWriter(2, newSlow(100*time.Millisecond)),
				},
				{
					name:    "4:4",
					data:    replicateByteslice(4, []byte(tearsInRain)),
					buffers: replicateWriter(4, newSlow(100*time.Millisecond)),
				},
				{
					name:    "10:512",
					data:    replicateByteslice(10, []byte(tearsInRain)),
					buffers: replicateWriter(512, newSlow(100*time.Millisecond)),
				},
			},
		},
	} {
		for _, sample := range errs.samples {
			t.Run(
				fmt.Sprintf("%s %T", sample.name, errs.err),
				func(t *testing.T) {
					called := false
					w := NewConcurrentMultiWriter(
						ConcurrentMultiWriterTestConfig,
						func(err error) {
							assert.IsType(t, errs.err, err)
							called = true
						},
						sample.buffers...,
					)
					defer w.Close()

					wg := &sync.WaitGroup{}
					wg.Add(len(sample.data))

					for _, v := range sample.data {
						go func(v []byte) {
							defer wg.Done()

							n, err := w.Write(v)

							assert.Equal(t, nil, err, sample.name)
							assert.Equal(t, len(v), n, sample.name)
						}(v)
					}

					wg.Wait()

					assert.Equal(t, 0, len(w.preempt))
					assert.Equal(t, len(sample.data) > 0 && len(sample.buffers) > 0, called)
				},
			)
		}
	}
}

func TestConcurrentMultiWriterAddWrite(t *testing.T) {
	var (
		samples = smallSeriesSamples(newBuffer)
	)

	for _, sample := range samples {
		t.Run(
			sample.name,
			func(t *testing.T) {
				w := NewConcurrentMultiWriter(
					ConcurrentMultiWriterTestConfig,
					func(err error) { panic(err) },
				)
				defer w.Close()

				testWrite := func(series [][]byte, buffers []io.Writer) {
					for k, v := range series {
						r := bytes.Join(series[:k+1], nil)
						n, err := w.Write(v)

						assert.Equal(t, nil, err)
						assert.Equal(t, len(v), n)
						assert.Equal(t, 0, len(w.preempt))

						for _, buf := range buffers {
							assert.Equal(
								t,
								r,
								buf.(*bytes.Buffer).Bytes(),
							)
						}
					}
				}

				for _, buf := range sample.buffers {
					w.Add(buf)
				}
				testWrite(sample.data, sample.buffers)
			},
		)
	}
}

func TestConcurrentMultiWriterAddWriteRemoveAllAddWrite(t *testing.T) {
	var (
		samples = smallSeriesSamples(newBuffer)
	)

	for _, sample := range samples {
		t.Run(
			sample.name,
			func(t *testing.T) {
				w := NewConcurrentMultiWriter(
					ConcurrentMultiWriterTestConfig,
					func(err error) { panic(err) },
				)
				defer w.Close()

				testWrite := func(series [][]byte, buffers []io.Writer) {
					for k, v := range series {
						r := bytes.Join(series[:k+1], nil)
						n, err := w.Write(v)

						assert.Equal(t, nil, err)
						assert.Equal(t, len(v), n)
						assert.Equal(t, 0, len(w.preempt))

						for _, buf := range buffers {
							assert.Equal(
								t,
								r,
								buf.(*bytes.Buffer).Bytes(),
							)
						}
					}
				}

				for _, buf := range sample.buffers {
					w.Add(buf)
				}
				testWrite(sample.data, sample.buffers)

				assert.Equal(t, w.RemoveAll(), len(sample.buffers) > 0)

				newBuffers := replicateWriter(uint(len(sample.buffers)), newBuffer)
				for _, buf := range newBuffers {
					w.Add(buf)
				}
				testWrite(sample.data, newBuffers)
			},
		)
	}
}

func TestConcurrentMultiWriterWriteRemoveWrite(t *testing.T) {
	var (
		samples = smallSeriesSamples(newBuffer)
	)

	for _, sample := range samples {
		t.Run(
			sample.name,
			func(t *testing.T) {
				w := NewConcurrentMultiWriter(
					ConcurrentMultiWriterTestConfig,
					func(err error) { panic(err) },
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
				assert.Equal(t, 0, len(w.preempt))

				for _, v := range sample.data {
					n, err := w.Write(v)
					assert.Equal(t, nil, err)
					assert.Equal(t, len(v), n)
					assert.Equal(t, 0, len(w.preempt))

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
		samples = smallSeriesSamples(newBuffer)
	)

	for _, sample := range samples {
		t.Run(
			sample.name,
			func(t *testing.T) {
				w := NewConcurrentMultiWriter(
					ConcurrentMultiWriterTestConfig,
					func(err error) { panic(err) },
				)
				defer w.Close()

				for _, buf := range sample.buffers {
					w.Add(buf)
				}

				for k, v := range sample.data {
					r := bytes.Join(sample.data[:k+1], nil)
					n, err := w.Write(v)

					assert.Equal(t, nil, err)
					assert.Equal(t, len(v), n)
					assert.Equal(t, 0, len(w.preempt))

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
				assert.Equal(t, 0, len(w.preempt))

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
	var (
		samples = benchmarkingSeriesSamples(newDiscard)
	)

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
