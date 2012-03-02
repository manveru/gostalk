package gostalk

import (
  "bufio"
)

type Conn interface {
  Close() error
  Read([]byte) (int, error)
  Write([]byte) (int, error)
}

type Client struct {
  server       *Server
  conn         Conn
  reader       Reader
  usedTube     *Tube
  watchedTubes map[string]*Tube
  isProducer bool // has issued at least one "put" command
  isWorker bool // has issued at least one "reserve" or "reserve-with-timeout" command
}

func newClient(server *Server, conn Conn) (client *Client) {
  client = &Client{
    server:       server,
    conn:         conn,
    reader:       bufio.NewReader(conn),
    watchedTubes: map[string]*Tube{},
  }

  client.useTube("default")
  client.watchTube("default")
  return
}

func (client *Client) onDisconnect() {
}

func (client *Client) useTube(name string) {
  client.usedTube = client.server.findOrCreateTube(name)
}

func (client *Client) watchTube(name string) {
  client.watchedTubes[name] = client.server.findOrCreateTube(name)
}

func (client *Client) ignoreTube(name string) {
  delete(client.watchedTubes, name)
}
