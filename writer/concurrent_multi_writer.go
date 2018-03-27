package writer

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/corpix/pool"
)

type writesStatus struct {
	target  uint32
	current uint32
	done    chan struct{}
}

// ConcurrentMultiWriterConfig is a configuration for ConcurrentMultiWriter.
// Backlog is a size of the writer personal buffer(if writer is slow or so).
// Pool is a configuration for github.com/corpix/pool
type ConcurrentMultiWriterConfig struct {
	Backlog int         `default:"64" validate:"required"`
	Pool    pool.Config `default:"{\"Workers\": 1024, \"QueueSize\": 16}" validate:"required"`
}

// ConcurrentMultiWriter represents a dynamic list of Writer interfaces.
type ConcurrentMultiWriter struct {
	config         ConcurrentMultiWriterConfig
	pool           *pool.Pool
	writers        []*WriterChannel
	writersLock    *sync.RWMutex
	preemptionLock *sync.RWMutex
	preemption     map[uintptr]*writesStatus
	errorHandler   func(*ConcurrentMultiWriter, io.Writer, error)
}

// Add adds a Writer to the ConcurrentMultiWriter.
// Could block if it runs out of (pool.Pool.QueueSize, which could mean we have
// no free slots in pool.Pool.Workers).
func (w *ConcurrentMultiWriter) Add(wr io.Writer) {
	var (
		wc = NewWriterChannel(
			wr,
			// XXX: Closing in Remove()
			make(chan *[]byte, w.config.Backlog),
		)
	)

	w.writersLock.Lock()
	defer w.writersLock.Unlock()

	w.pool.Feed <- pool.NewWork(
		context.Background(),
		w.channelWriterPump(wc),
	)

	w.writers = append(w.writers, wc)
}

// TryAdd try add a a Writer to the ConcurrentMultiWriter without blocking.
func (w *ConcurrentMultiWriter) TryAdd(wr io.Writer) bool {
	var (
		wc = NewWriterChannel(
			wr,
			// XXX: Closing in Remove()
			make(chan *[]byte, w.config.Backlog),
		)
	)

	w.writersLock.Lock()
	defer w.writersLock.Unlock()

	select {
	case w.pool.Feed <- pool.NewWork(
		context.Background(),
		w.channelWriterPump(wc),
	):
	default:
		return false
	}

	w.writers = append(w.writers, wc)

	return true
}

// Remove removes Writer from the list if it exists
// and returns true in this case, otherwise it will be
// false.
func (w *ConcurrentMultiWriter) Remove(c io.Writer) bool {
	w.writersLock.Lock()
	defer w.writersLock.Unlock()

	for k, wr := range w.writers {
		if wr.Writer == c {
			// XXX: Created in Add()
			defer close(wr.Channel)

			if k < len(w.writers)-1 {
				w.writers = append(
					w.writers[:k],
					w.writers[k+1:]...,
				)
			} else {
				w.writers = w.writers[:k]
			}
			return true
		}
	}

	return false
}

// RemoveAll removes all writers.
func (w *ConcurrentMultiWriter) RemoveAll() bool {
	w.writersLock.Lock()
	defer w.writersLock.Unlock()

	res := len(w.writers) > 0

	for _, wr := range w.writers {
		// XXX: Created in Add()
		defer close(wr.Channel)
	}
	w.writers = nil

	return res
}

// Has returns true if Writer exists in the list and false otherwise.
func (w *ConcurrentMultiWriter) Has(c io.Writer) bool {
	w.writersLock.RLock()
	defer w.writersLock.RUnlock()

	for _, wr := range w.writers {
		if wr.Writer == c {
			return true
		}
	}

	return false
}

func (w *ConcurrentMultiWriter) channelWriterPump(wr *WriterChannel) pool.Executor {
	return func(ctx context.Context) {
		var (
			n       int
			buf     *[]byte
			preempt *writesStatus
			err     error
		)

	prelude:
		select {
		case <-ctx.Done():
			return
		case buf = <-wr.Channel:
			goto head
		}

	head:
		if buf == nil {
			return
		}

		n, err = wr.Writer.Write(*buf)
		if err != nil {
			w.errorHandler(w, wr, err)
			goto tail
		}

		if n < len(*buf) {
			w.errorHandler(w, wr, io.ErrShortWrite)
			goto tail
		}

	tail:
		w.preemptionLock.RLock()
		preempt = w.preemption[uintptr(unsafe.Pointer(buf))]
		atomic.AddUint32(&preempt.current, 1)
		if atomic.CompareAndSwapUint32(&preempt.current, preempt.target, 0) {
			close(preempt.done)
		}
		w.preemptionLock.RUnlock()

		goto prelude
	}
}

// Write writes a buf to each concrete writer concurrently.
// This writer is «eventually consistent», it doesn't wait until
// all data will be sent before return control.
func (w *ConcurrentMultiWriter) Write(buf []byte) (int, error) {
	var (
		done        = make(chan struct{})
		errHandlers []func()
	)

	w.writersLock.RLock()

	if len(w.writers) == 0 {
		w.writersLock.RUnlock()
		return len(buf), nil
	}

	status := &writesStatus{
		target:  uint32(len(w.writers)),
		current: 0,
		done:    done,
	}

	w.preemptionLock.Lock()
	w.preemption[uintptr(unsafe.Pointer(&buf))] = status
	w.preemptionLock.Unlock()

	for _, wr := range w.writers {
		select {
		case wr.Channel <- &buf:
		default:
			errHandlers = append(
				errHandlers,
				(func(wr io.Writer) func() {
					return func() {
						w.errorHandler(w, wr, NewErrBacklogOverflow(w.config.Backlog))
					}
				}(wr)),
			)
		}
	}

	w.writersLock.RUnlock()

	if len(errHandlers) > 0 {
		for _, errHandler := range errHandlers {
			errHandler()
		}
	}

	<-done

	w.preemptionLock.Lock()
	delete(w.preemption, uintptr(unsafe.Pointer(&buf)))
	w.preemptionLock.Unlock()

	return len(buf), nil
}

func (w *ConcurrentMultiWriter) Close() error {
	w.pool.Close()

	w.preemptionLock.Lock()
	defer w.preemptionLock.Unlock()

	for _, preempt := range w.preemption {
		close(preempt.done)
	}
	w.preemption = nil

	w.RemoveAll()

	return nil
}

// NewConcurrentMultiWriter creates new Writer.
func NewConcurrentMultiWriter(c ConcurrentMultiWriterConfig, errorHandler func(*ConcurrentMultiWriter, io.Writer, error), ws ...io.Writer) *ConcurrentMultiWriter {
	var (
		w = &ConcurrentMultiWriter{
			config:         c,
			pool:           pool.NewFromConfig(c.Pool),
			writersLock:    &sync.RWMutex{},
			preemptionLock: &sync.RWMutex{},
			preemption:     map[uintptr]*writesStatus{},
			errorHandler:   errorHandler,
		}
	)

	for _, wr := range ws {
		w.Add(wr)
	}

	return w
}
