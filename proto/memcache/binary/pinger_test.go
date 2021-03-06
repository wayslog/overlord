package binary

import (
	"testing"

	"overlord/lib/bufio"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPingerPingOk(t *testing.T) {
	conn := _createConn(pongBs)
	pinger := newMCPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)
}

func TestPingerPingEOF(t *testing.T) {
	conn := _createConn(pongBs)
	pinger := newMCPinger(conn)

	err := pinger.Ping()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.Error(t, err)

	err = errors.Cause(err)
	assert.Equal(t, bufio.ErrBufferFull, err)
}

func TestPingerPing100Ok(t *testing.T) {
	conn := _createRepeatConn(pongBs, 100)
	pinger := newMCPinger(conn)

	for i := 0; i < 100; i++ {
		err := pinger.Ping()
		assert.NoError(t, err, "error iter: %d", i)
	}

	err := pinger.Ping()
	assert.Error(t, err)
	_causeEqual(t, bufio.ErrBufferFull, err)
}

func TestPingerClosed(t *testing.T) {
	conn := _createRepeatConn(pongBs, 100)
	pinger := newMCPinger(conn)
	err := pinger.Close()
	assert.NoError(t, err)

	err = pinger.Ping()
	assert.Error(t, err)
	assert.NoError(t, pinger.Close())
}

func TestPingerNotReturnPong(t *testing.T) {
	conn := _createConn([]byte("iam test bytes 24 length"))
	pinger := newMCPinger(conn)
	err := pinger.Ping()
	assert.Error(t, err)
	_causeEqual(t, ErrPingerPong, err)

	conn = _createConn([]byte("less than 24 length"))
	pinger = newMCPinger(conn)
	err = pinger.Ping()
	assert.Error(t, err)
	_causeEqual(t, bufio.ErrBufferFull, err)
}
