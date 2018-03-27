package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/corpix/effects/writer"
	"github.com/corpix/pool"
)

func main() {
	var (
		cmr = writer.NewConcurrentMultiWriter(
			writer.ConcurrentMultiWriterConfig{
				Backlog: 8,
				Pool: pool.Config{
					Workers:   128,
					QueueSize: 8,
				},
			},
			func(cmr *writer.ConcurrentMultiWriter, w io.Writer, err error) {
				panic(err)
			},
		)
		buf = bytes.NewBuffer(nil)
		msg = []byte("** hello **\n")

		n   int
		err error
	)
	defer cmr.Close()

	cmr.Add(buf)
	cmr.Add(os.Stdout)

	fmt.Println("Writing message into buffer and into stdout...")

	n, err = cmr.Write(msg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Wrote %d bytes\n", n)
	fmt.Printf("Buffer msg is %d in len\n", len(buf.Bytes()))
	fmt.Printf("Original msg len is %d\n", len(msg))
}
