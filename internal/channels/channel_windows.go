package channels

import (
	"github.com/cuihairu/simplegoserver/pkg/channels"
	"golang.org/x/sys/windows"
	"log"
	"net"
	"time"
)

type SocketChannel struct {
	conn net.Conn
}

func NewSocketChannel(conn net.Conn) *SocketChannel {
	return &SocketChannel{conn}
}

type SocketAcceptor struct {
	net.Listener
	iocpHandle windows.Handle
}

func NewAcceptor(network string, address string) (*SocketAcceptor, error) {
	l, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	return &SocketAcceptor{
		Listener: l,
	}, nil
}

func (s *SocketAcceptor) Accept() (net.Conn, error) {
	return s.Listener.Accept()
}

func (s *SocketAcceptor) Close() error {
	err := windows.CloseHandle(s.iocpHandle)
	if err != nil {
		return err
	}
	return s.Listener.Close()
}

type SocketSelector struct {
	iocpHandle windows.Handle
	selectKeys channels.SelectKeys
}

func (s *SocketSelector) Select() ([]channels.Channel, error) {
	//TODO implement me
	panic("implement me")
}

func (s *SocketSelector) SelectWithTimeout(timeout time.Duration) ([]channels.Channel, error) {
	//TODO implement me
	panic("implement me")
}

func (s *SocketSelector) SelectNow() ([]channels.Channel, error) {
	//TODO implement me
	panic("implement me")
}

func (s *SocketSelector) Wakeup() {
	//TODO implement me
	panic("implement me")
}

func (s *SocketSelector) SelectKeys() channels.SelectKeys {
	//TODO implement me
	panic("implement me")
}

func (s *SocketSelector) Close() error {
	err := windows.CloseHandle(s.iocpHandle)
	if err != nil {
		return err
	}

	return nil
}

var _ channels.Selector = (*SocketSelector)(nil)

func NewSelector(keys channels.SelectKeys) (*SocketSelector, error) {
	iocp, err := windows.CreateIoCompletionPort(windows.InvalidHandle, 0, 0, 1)
	if err != nil {
		log.Fatal(err)
	}
	return &SocketSelector{
		iocpHandle: iocp,
	}, nil
}
