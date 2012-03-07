package gostalk

import (
	"bufio"
)

type conn interface {
	Close() error
	Read([]byte) (int, error)
	Write([]byte) (int, error)
}

type client struct {
	server       *server
	conn         conn
	reader       reader
	usedTube     *tube
	watchedTubes map[string]*tube
	isProducer   bool // has issued at least one "put" command
	isWorker     bool // has issued at least one "reserve" or "reserve-with-timeout" command
}

func newClient(server *server, conn conn) *client {
	c := &client{
		server:       server,
		conn:         conn,
		reader:       bufio.NewReader(conn),
		watchedTubes: map[string]*tube{},
	}

	c.useTube("default")
	c.watchTube("default")
	return c
}

func (client *client) useTube(name string) {
	client.usedTube = client.server.findOrCreateTube(name)
}

func (client *client) watchTube(name string) {
	client.watchedTubes[name] = client.server.findOrCreateTube(name)
}

func (client *client) ignoreTube(name string) (ignored bool, totalTubes int) {
	totalTubes = len(client.watchedTubes)

	if totalTubes > 1 {
		delete(client.watchedTubes, name)
		return true, totalTubes - 1
	}

	return false, totalTubes
}
