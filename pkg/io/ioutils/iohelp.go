package ioutils

import (
    "bufio"
    "compress/gzip"
    "errors"
    "io"
    "os"
    "path/filepath"
)

// OpenMaybeCompressed opens a file path or stdin ("-") and returns a reader.
// If the input appears to be gzip (by extension or magic), it wraps with gzip.
func OpenMaybeCompressed(path string) (io.ReadCloser, error) {
    if path == "-" || path == "" {
        // sniff gzip from stdin
        br := bufio.NewReader(os.Stdin)
        b, err := br.Peek(2)
        if err == nil && len(b) >= 2 && b[0] == 0x1f && b[1] == 0x8b {
            zr, err := gzip.NewReader(br)
            if err != nil { return nil, err }
            return zr, nil
        }
        return io.NopCloser(br), nil
    }
    f, err := os.Open(path)
    if err != nil { return nil, err }
    // extension check
    if ext := filepath.Ext(path); ext == ".gz" {
        zr, err := gzip.NewReader(f)
        if err != nil { _ = f.Close(); return nil, err }
        // return a ReadCloser that closes both
        return readCloser{Reader: zr, closeFn: func() error { _ = zr.Close(); return f.Close() }}, nil
    }
    // sniff magic
    br := bufio.NewReader(f)
    b, err := br.Peek(2)
    if err == nil && len(b) >= 2 && b[0] == 0x1f && b[1] == 0x8b {
        zr, err := gzip.NewReader(br)
        if err != nil { _ = f.Close(); return nil, err }
        return readCloser{Reader: zr, closeFn: func() error { _ = zr.Close(); return f.Close() }}, nil
    }
    return readCloser{Reader: br, closeFn: f.Close}, nil
}

// CreateMaybeCompressed creates a file (or stdout if path is "-") and
// returns a writer. If the path ends in .gz, the writer is gzip compressed.
func CreateMaybeCompressed(path string) (io.WriteCloser, error) {
    if path == "-" || path == "" {
        // stdout: cannot detect compression; write plain
        return nopWriteCloser{Writer: bufio.NewWriter(os.Stdout)}, nil
    }
    f, err := os.Create(path)
    if err != nil { return nil, err }
    if filepath.Ext(path) == ".gz" {
        zw := gzip.NewWriter(f)
        return writeCloser{Writer: zw, closeFn: func() error { _ = zw.Close(); return f.Close() }}, nil
    }
    return writeCloser{Writer: bufio.NewWriter(f), closeFn: f.Close}, nil
}

type readCloser struct{
    io.Reader
    closeFn func() error
}
func (r readCloser) Close() error {
    if r.closeFn != nil { return r.closeFn() }
    return errors.New("no closeFn")
}

type writeCloser struct{
    io.Writer
    closeFn func() error
}
func (w writeCloser) Close() error {
    if bw, ok := w.Writer.(*bufio.Writer); ok { _ = bw.Flush() }
    if w.closeFn != nil { return w.closeFn() }
    return errors.New("no closeFn")
}

type nopWriteCloser struct{ io.Writer }
func (n nopWriteCloser) Close() error {
    if bw, ok := n.Writer.(*bufio.Writer); ok { return bw.Flush() }
    return nil
}

