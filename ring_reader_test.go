package effects

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRingReader(t *testing.T) {
	samples := []struct {
		name   string
		seed   []byte
		buf    []byte
		result []byte
	}{
		{
			name:   "seed larger than buf",
			seed:   []byte("I've seen things you people wouldn't believe. Attack ships on fire off the shoulder of Orion. I watched C-beams glitter in the dark near the Tannhäuser Gate. All those moments will be lost in time, like tears in rain. Time to die."),
			buf:    make([]byte, 45),
			result: []byte("I've seen things you people wouldn't believe."),
		},
		{
			name:   "seed same as buf",
			seed:   []byte("I've seen things you people wouldn't believe."),
			buf:    make([]byte, 45),
			result: []byte("I've seen things you people wouldn't believe."),
		},
		{
			name:   "seed repeats in buf",
			seed:   []byte("I've seen things you people wouldn't believe."),
			buf:    make([]byte, 90),
			result: []byte("I've seen things you people wouldn't believe.I've seen things you people wouldn't believe."),
		},
		{
			name:   "seed part repeats in buf",
			seed:   []byte("I've seen things you people wouldn't believe."),
			buf:    make([]byte, 80),
			result: []byte("I've seen things you people wouldn't believe.I've seen things you people wouldn'"),
		},
	}

	for _, sample := range samples {
		t.Run(
			sample.name,
			func(t *testing.T) {
				_, err := NewRingReader(sample.seed).
					Read(sample.buf)
				if err != nil {
					t.Error(err)
					return
				}

				assert.Equal(t, sample.result, sample.buf)
			},
		)
	}
}
