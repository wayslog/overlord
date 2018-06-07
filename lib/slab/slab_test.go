package slab

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadByteOk(t *testing.T) {
	rd := bytes.NewBuffer([]byte("abcdef"))
	buf := NewReader(rd, 1)
	d, err := buf.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, byte('a'), d)
	assert.True(t, buf.isFull())
	assert.Len(t, buf.buf, 1)
	assert.Equal(t, 1, buf.acquired)
	assert.Equal(t, 1, buf.readed)
	assert.Equal(t, 0, buf.free)

	d, err = buf.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, byte('b'), d)
	assert.Len(t, buf.buf, 4)
	assert.Equal(t, 2, buf.acquired)
	assert.Equal(t, 2, buf.readed)
	assert.Equal(t, 0, buf.free)

	d, err = buf.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, byte('c'), d)
	assert.Len(t, buf.buf, 4)
	assert.Equal(t, 3, buf.acquired)
	assert.Equal(t, 3, buf.readed)
	assert.Equal(t, 0, buf.free)

	buf.Release(1)
	assert.Len(t, buf.buf, 4)
	assert.Equal(t, 3, buf.acquired)
	assert.Equal(t, 3, buf.readed)
	assert.Equal(t, 1, buf.free)

	d, err = buf.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, byte('d'), d)
	assert.Len(t, buf.buf, 4)

	assert.Equal(t, 4, buf.acquired)
	assert.Equal(t, 4, buf.readed)
	assert.Equal(t, 1, buf.free)

	d, err = buf.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, byte('e'), d)
	assert.Len(t, buf.buf, 4)
	assert.Equal(t, 4, buf.acquired)
	assert.Equal(t, 4, buf.readed)
	assert.Equal(t, 0, buf.free)
}

func TestReadBytesOk(t *testing.T) {
	rd := bytes.NewBuffer([]byte("abcdef"))
	buf := NewReader(rd, 1)

	s, err := buf.ReadBytes(byte('d'))
	assert.NoError(t, err)
	assert.Equal(t, []byte("abcd"), s)

	s, err = buf.ReadBytes(byte('f'))
	assert.NoError(t, err)
	assert.Equal(t, []byte("ef"), s)
	s, err = buf.ReadBytes(byte('a'))
	assert.Equal(t, io.EOF, err)
	assert.Len(t, s, 0)
}

func TestReadFullOk(t *testing.T) {
	rd := bytes.NewBuffer([]byte("abcdef"))
	buf := NewReader(rd, 1)

	s, err := buf.ReadFull(3)
	assert.NoError(t, err)
	assert.Equal(t, []byte("abc"), s)
	s, err = buf.ReadFull(10)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []byte("def"), s)
}
