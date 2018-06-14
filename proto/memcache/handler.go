package memcache

import (
	"bytes"
	"net"
	"sync/atomic"
	"time"

	"github.com/felixhao/overlord/lib/bufio"
	"github.com/felixhao/overlord/lib/conv"
	"github.com/felixhao/overlord/lib/pool"
	"github.com/felixhao/overlord/lib/stat"
	"github.com/felixhao/overlord/proto"
	"github.com/pkg/errors"
)

const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)

	handlerWriteBufferSize = 8 * 1024   // NOTE: write command, so relatively small
	handlerReadBufferSize  = 128 * 1024 // NOTE: read data, so relatively large
)

type handler struct {
	cluster string
	addr    string
	conn    net.Conn
	br      *bufio.Reader
	bw      *bufio.Writer
	bss     [][]byte

	readTimeout  time.Duration
	writeTimeout time.Duration

	closed int32
}

// Dial returns pool Dial func.
func Dial(cluster, addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (dial func() (pool.Conn, error)) {
	dial = func() (pool.Conn, error) {
		conn, err := net.DialTimeout("tcp", addr, dialTimeout)
		if err != nil {
			return nil, err
		}
		h := &handler{
			cluster:      cluster,
			addr:         addr,
			conn:         conn,
			bw:           bufio.NewWriterSize(conn, handlerWriteBufferSize),
			br:           bufio.NewReaderSize(conn, handlerReadBufferSize),
			bss:          make([][]byte, 3), // NOTE: like: 'VALUE a_11 0 0 3\r\naaa\r\nEND\r\n'
			readTimeout:  readTimeout,
			writeTimeout: writeTimeout,
		}
		return h, nil
	}
	return
}

// Handle call server node by Msg and read response returned.
func (h *handler) Handle(req *proto.Msg) (err error) {
	if h.Closed() {
		err = errors.Wrap(ErrClosed, "MC Handler handle Msg")
		return
	}
	mcr, ok := req.Proto().(*MCMsg)
	if !ok {
		err = errors.Wrap(ErrAssertMsg, "MC Handler handle assert MCMsg")
		return
	}
	if h.writeTimeout > 0 {
		h.conn.SetWriteDeadline(time.Now().Add(h.writeTimeout))
	}
	h.bw.WriteString(mcr.rTp.String())
	h.bw.WriteByte(spaceByte)
	if mcr.rTp == MsgTypeGat || mcr.rTp == MsgTypeGats {
		h.bw.Write(mcr.data) // NOTE: exptime
		h.bw.WriteByte(spaceByte)
		h.bw.Write(mcr.key)
		h.bw.Write(crlfBytes)
	} else {
		h.bw.Write(mcr.key)
		h.bw.Write(mcr.data)
	}
	if err = h.bw.Flush(); err != nil {
		err = errors.Wrap(err, "MC Handler handle flush Msg bytes")
		return
	}
	if h.readTimeout > 0 {
		h.conn.SetReadDeadline(time.Now().Add(h.readTimeout))
	}
	bss := make([][]byte, 2)
	bs, err := h.br.ReadBytes(delim)
	if err != nil {
		err = errors.Wrap(err, "MC Handler handle read response bytes")
		return
	}
	bss[0] = bs
	if mcr.rTp == MsgTypeGet || mcr.rTp == MsgTypeGets || mcr.rTp == MsgTypeGat || mcr.rTp == MsgTypeGats {
		if !bytes.Equal(bs, endBytes) {
			stat.Hit(h.cluster, h.addr)
			c := bytes.Count(bs, spaceBytes)
			if c < 3 {
				err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes split")
				return
			}
			var (
				lenBs  []byte
				length int64
			)
			i := bytes.IndexByte(bs, spaceByte) + 1 // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
			i = i + bytes.IndexByte(bs[i:], spaceByte) + 1
			i = i + bytes.IndexByte(bs[i:], spaceByte) + 1
			if c == 3 { // NOTE: if c==3, means get|gat
				lenBs = bs[i:]
				l := len(lenBs)
				if l < 2 {
					err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes check")
					return
				}
				lenBs = lenBs[:l-2] // NOTE: get|gat contains '\r\n'
			} else { // NOTE: if c>3, means gets|gats
				j := i + bytes.IndexByte(bs[i:], spaceByte)
				lenBs = bs[i:j]
			}
			if length, err = conv.Btoi(lenBs); err != nil {
				err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes length")
				return
			}
			var bs2 []byte
			if bs2, err = h.br.ReadFull(int(length + 2)); err != nil { // NOTE: +2 read contains '\r\n'
				err = errors.Wrap(ErrBadResponse, "MC Handler handle read response bytes read")
				return
			}
			bss[1] = bs2
			var bs3 []byte
			for !bytes.Equal(bs3, endBytes) {
				if bs3 != nil { // NOTE: here, avoid copy 'END\r\n'
					bss = append(bss, bs3)
				}
				if h.readTimeout > 0 {
					h.conn.SetReadDeadline(time.Now().Add(h.readTimeout))
				}
				if bs3, err = h.br.ReadBytes(delim); err != nil {
					err = errors.Wrap(err, "MC Handler handle reread response bytes")
					return
				}
			}
			bss = append(bss, endBytes)
		} else {
			stat.Miss(h.cluster, h.addr)
		}
	}
	mcr.resp = bss
	return
}

func (h *handler) Close() error {
	if atomic.CompareAndSwapInt32(&h.closed, handlerOpening, handlerClosed) {
		return h.conn.Close()
	}
	return nil
}

func (h *handler) Closed() bool {
	return atomic.LoadInt32(&h.closed) == handlerClosed
}
