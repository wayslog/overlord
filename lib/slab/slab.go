// Package slab is the recycyle used buffer pool
package slab

import (
	"bufio"
	"bytes"
	"io"
)

const (
	growFactor = 2
	allocInf   = -1
)

// Reader is the struct like bufio
type Reader struct {
	rd io.Reader

	buf []byte

	free     int
	acquired int
	readed   int
}

// NewReader will create new cycle using reader
func NewReader(rd io.Reader, size int) *Reader {
	return &Reader{
		rd:  rd,
		buf: make([]byte, size),
	}
}

func minInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func (r *Reader) grow() {
	r.growSize(len(r.buf))
}

func (r *Reader) growSize(size int) {
	except := r.acquired - r.free + size
	cap := len(r.buf)
	for cap <= except {
		cap = cap * growFactor
	}

	buf := make([]byte, cap)
	copy(buf, r.buf[r.free:r.acquired])
	r.acquired = r.acquired - r.free
	r.readed = r.readed - r.free
	r.free = 0
	r.buf = buf
}

func (r *Reader) shrink() {
	copy(r.buf, r.buf[r.free:r.acquired])
	r.acquired = r.acquired - r.free
	r.readed = r.readed - r.free
	r.free = 0
}

func (r *Reader) hasUnreaded() bool {
	return r.readed < r.acquired
}

func (r *Reader) isFull() bool {
	return len(r.buf) == r.acquired
}

func (r *Reader) allocBuf(size int) []byte {
	buf := r.buf[r.acquired : r.acquired+size]
	r.acquired += size
	return buf
}

func (r *Reader) alloc(size int) []byte {
	remaining := len(r.buf) - r.acquired
	if size == allocInf {
		if r.isFull() {
			r.grow()
		}
		return r.allocBuf(remaining)
	}

	if remaining < size {
		if remaining+r.free < size {
			r.growSize(size)
		} else {
			r.shrink()
		}
	}
	return r.allocBuf(size)
}

func (r *Reader) releaseTop(size int) {
	r.acquired -= size
}

func (r *Reader) tryReadBufFull() error {
	b := r.alloc(allocInf)
	size, err := r.rd.Read(b)
	if err != nil {
		return err
	}
	r.releaseTop(len(b) - size)
	return nil
}

func (r *Reader) tryReadExact(n int) ([]byte, error) {
	b := r.alloc(n)
	count := 0
	for count != len(b) {
		tmp := b[count:]
		size, err := r.rd.Read(tmp)
		count += size
		if err != nil {
			r.releaseTop(n - count)
			return b[:count], err
		}
	}
	return b, nil
}

// Release will release memory buffer
func (r *Reader) Release(size int) {
	r.free = minInt(r.free+size, r.readed)
}

// ReadByte reads and returns a single byte.
// If no byte is available, returns an error.
func (r *Reader) ReadByte() (byte, error) {
	if r.hasUnreaded() {
		ret := r.buf[r.readed]
		r.readed++
		return ret, nil
	}

	b := r.alloc(1)
	_, err := r.rd.Read(b)

	if err == io.EOF {
		return b[0], err
	} else if err != nil {
		return 0, err
	}

	r.readed++
	return b[0], err
}

func (r *Reader) unreaded() []byte {
	return r.buf[r.readed:r.acquired]
}

// ReadSlice reads until the first occurrence of delim in the input,
// returning a slice pointing at the bytes in the buffer.
// The bytes stop being valid at the next read.
// If ReadSlice encounters an error before finding a delimiter,
// it returns all the data in the buffer and the error itself (often io.EOF).
// ReadSlice fails with error ErrBufferFull if the buffer fills without a delim.
// Because the data returned from ReadSlice will be overwritten
// by the next I/O operation, most clients should use ReadBytes instead.
// ReadSlice returns err != nil if and only if line does not end in delim.
func (r *Reader) ReadSlice(delim byte) ([]byte, error) {
	for {
		// read from buf[readed: acquired]
		pos := bytes.IndexByte(r.unreaded(), delim)
		if pos == -1 {
			if r.isFull() {
				if r.free == 0 {
					return r.unreaded(), bufio.ErrBufferFull
				}
				r.shrink()
			}

			err := r.tryReadBufFull()
			if err != nil && err == io.EOF {
				return nil, err
			}
			continue
		}

		buf := r.buf[r.readed : r.readed+pos+1]
		r.readed += pos + 1
		return buf, nil
	}
}

// ReadBytes reads until the first occurrence of delim in the input,
// returning a slice containing the data up to and including the delimiter.
// it returns the data read before the error and the error itself (often io.EOF).
// ReadBytes returns err != nil if and only if the returned data does not end in
// delim.
// For simple uses, a Scanner may be more convenient.
func (r *Reader) ReadBytes(delim byte) ([]byte, error) {
	// read from buf[readed: acquired] and read from bytes
	for {
		// read from buf[readed: acquired]
		pos := bytes.IndexByte(r.unreaded(), delim)
		if pos == -1 {
			if r.isFull() {
				r.grow()
			}

			err := r.tryReadBufFull()
			if err != nil && err == io.EOF {
				return nil, err
			}
			continue
		}

		buf := r.buf[r.readed : r.readed+pos+1]
		r.readed += pos + 1
		return buf, nil
	}

}

// ReadFull reads exactly n bytes from r into buf.
// It returns the number of bytes copied and an error if fewer bytes were read.
// The error is EOF only if no bytes were read.
// If an EOF happens after reading some but not all the bytes,
// ReadFull returns ErrUnexpectedEOF.
// On return, n == len(buf) if and only if err == nil.
func (r *Reader) ReadFull(n int) ([]byte, error) {
	// read from [readed: acquired] and read from bytes
	return r.tryReadExact(n)
}
