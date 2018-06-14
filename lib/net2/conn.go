package net2

import (
	"net"
	"time"
)

// Conn is a net.Conn self implement
// Add auto timeout setting.
type Conn struct {
	sock net.Conn

	readerTimeout time.Duration
	writerTimeout time.Duration

	hasReadDeadline  bool
	hasWriteDeadline bool

	LastWrite time.Time
}

// DialWithTimeout will create new auto timeout Conn
func DialWithTimeout(addr string, dialTimeout, readerTimeout, writerTimeout time.Duration) (*Conn, error) {
	socket, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	return NewConn(socket, readerTimeout, writerTimeout), nil
}

// NewConn will create new Connection with given socket
func NewConn(sock net.Conn, readerTimeout, writerTimeout time.Duration) *Conn {
	Conn := &Conn{sock: sock, readerTimeout: readerTimeout, writerTimeout: writerTimeout}
	return Conn
}

// LocalAddr impl net.Conn
func (c *Conn) LocalAddr() net.Addr {
	return c.sock.LocalAddr()
}

// RemoteAddr impl net.Conn
func (c *Conn) RemoteAddr() net.Addr {
	return c.sock.RemoteAddr()
}

// Close impl net.Conn and io.Closer
func (c *Conn) Close() error {
	return c.sock.Close()
}

// SetDeadline sets the read and write deadlines associated
// sockets.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.sock.SetDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls
// and any currently-blocked Read call.
// A zero value for t means Read will not time out.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.sock.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that
// some of the data was successfully written.
// A zero value for t means Write will not time out.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.sock.SetWriteDeadline(t)
}

// CloseReader will close the real tcp reader window.
func (c *Conn) CloseReader() error {
	if t, ok := c.sock.(*net.TCPConn); ok {
		return t.CloseRead()
	}
	return c.Close()
}

func (c *Conn) Read(b []byte) (int, error) {
	if timeout := c.readerTimeout; timeout != 0 {
		if err := c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return 0, err
		}
		c.hasReadDeadline = true
	} else if c.hasReadDeadline {
		if err := c.SetReadDeadline(time.Time{}); err != nil {
			return 0, err
		}
		c.hasReadDeadline = false
	}
	return c.sock.Read(b)
}

func (c *Conn) Write(b []byte) (int, error) {
	if timeout := c.writerTimeout; timeout != 0 {
		if err := c.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return 0, err
		}
		c.hasWriteDeadline = true
	} else if c.hasWriteDeadline {
		if err := c.SetWriteDeadline(time.Time{}); err != nil {
			return 0, err
		}
		c.hasWriteDeadline = false
	}
	n, err := c.sock.Write(b)
	if err != nil {
		return n, err
	}
	c.LastWrite = time.Now()
	return n, err
}

// writeBuffers impl the net.buffersWriter to support writev
func (c *Conn) writeBuffers(buf *net.Buffers) (int64, error) {
	return buf.WriteTo(c.sock)
}
