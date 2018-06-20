package bufio

import (
	"bytes"
	"io"
	"net"
)

// Reader implements buffering for an io.Reader object.
type Reader struct {
	rd io.Reader
	b  *Buffer
}

// NewReader returns a new Reader whose buffer has the default size.
func NewReader(rd io.Reader, b *Buffer) *Reader {
	return &Reader{rd: rd, b: b}
}

func (r *Reader) fill() error {
	n, err := r.rd.Read(r.b.buf[r.b.w:])
	if err != nil {
		return err
	} else if n == 0 {
		return io.ErrNoProgress
	} else {
		r.b.w += n
	}
	return nil
}

// Advance proxy to buffer advance
func (r *Reader) Advance(n int) {
	r.b.Advance(n)
}

// Buffer will return the reference of local buffer
func (r *Reader) Buffer() *Buffer {
	return r.b
}

// ResetBuffer reset buf.
func (r *Reader) ResetBuffer(b *Buffer) {
	b.Reset()
	n := 0
	if r.b != nil {
		if r.b.buffered() > 0 {
			n = copy(b.buf, r.b.buf[r.b.r:r.b.w])
		}
		Put(r.b)
	}
	r.b = b
	r.b.w = n
	r.b.r = 0
}

// ReadUntil reads until the first occurrence of delim in the input,
// returning a slice pointing at the bytes in the buffer.
// The bytes stop being valid at the next read.
// If ReadUntil encounters an error before finding a delimiter,
// it returns all the data in the buffer and the error itself (often io.EOF).
// ReadUntil returns err != nil if and only if line does not end in delim.
func (r *Reader) ReadUntil(delim byte) ([]byte, error) {
	for {
		var index = bytes.IndexByte(r.b.buf[r.b.r:r.b.w], delim)
		if index >= 0 {
			limit := r.b.r + index + 1
			slice := r.b.buf[r.b.r:limit]
			r.b.r = limit
			return slice, nil
		}
		if r.b.w >= r.b.len() {
			r.b.grow()
		}
		err := r.fill()
		if err != nil {
			return nil, err
		}
	}
}

// ReadFull reads exactly n bytes from r into buf.
// It returns the number of bytes copied and an error if fewer bytes were read.
// The error is EOF only if no bytes were read.
// If an EOF happens after reading some but not all the bytes,
// ReadFull returns ErrUnexpectedEOF.
// On return, n == len(buf) if and only if err == nil.
func (r *Reader) ReadFull(n int) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	for {
		if r.b.buffered() >= n {
			bs := r.b.buf[r.b.r : r.b.r+n]
			r.b.r += n
			return bs, nil
		}
		maxCanRead := r.b.len() - r.b.w + r.b.buffered()
		if maxCanRead < n {
			r.b.grow()
		}
		if err := r.fill(); err != nil && err != io.ErrNoProgress {
			return nil, err
		}
	}
}

// Writer implements buffering for an io.Writer object.
// If an error occurs writing to a Writer, no more data will be
// accepted and all subsequent writes, and Flush, will return the error.
// After all data has been written, the client should call the
// Flush method to guarantee all data has been forwarded to
// the underlying io.Writer.
type Writer struct {
	wr   io.Writer
	bufs net.Buffers

	err error
}

// NewWriter returns a new Writer whose buffer has the default size.
func NewWriter(wr io.Writer) *Writer {
	return &Writer{wr: wr, bufs: net.Buffers(make([][]byte, 0, 128))}
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if len(w.bufs) == 0 {
		return nil
	}
	_, err := w.bufs.WriteTo(w.wr)
	if err != nil {
		w.err = err
	}
	w.bufs = w.bufs[0:0]
	return w.err
}

// Write writes the contents of p into the buffer.
// It returns the number of bytes written.
// If nn < len(p), it also returns an error explaining
// why the write is short.
func (w *Writer) Write(p []byte) (err error) {
	if w.err != nil {
		return w.err
	}
	if p == nil {
		return nil
	}

	if len(w.bufs) == 10 {
		w.Flush()
	}
	w.bufs = append(w.bufs, p)
	return nil
}

// WriteString writes a string.
// It returns the number of bytes written.
// If the count is less than len(s), it also returns an error explaining
// why the write is short.
func (w *Writer) WriteString(s string) (err error) {
	return w.Write([]byte(s))
}