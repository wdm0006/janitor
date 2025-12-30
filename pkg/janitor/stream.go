package janitor

import (
	"context"
	"io"
)

// ChunkSource yields frames in chunks until io.EOF.
type ChunkSource interface {
	Next() (*Frame, error)
}

// ChunkSink consumes frames, typically writing them out.
type ChunkSink interface {
	Write(*Frame) error
	Close() error
}

// RunStream pulls chunks from src, applies the pipeline, and writes to sink.
func RunStream(ctx context.Context, p *Pipeline, src ChunkSource, sink ChunkSink) error {
    defer func() { _ = sink.Close() }()
	for {
		f, err := src.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		out, err := p.Run(ctx, f)
		if err != nil {
			return err
		}
		if err := sink.Write(out); err != nil {
			return err
		}
	}
}
