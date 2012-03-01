package gostalker

import (
  "bufio"
)

type Conn interface {
  Close() error
  Read([]byte) (int, error)
  Write([]byte) (int, error)
}

type Client struct {
  conn         Conn
  reader       Reader
  usedTube     *Tube
  watchedTubes []*Tube
}

func newClient(conn Conn, tube *Tube) (client *Client) {
  client = &Client{
    conn:         conn,
    reader:       bufio.NewReader(conn),
    usedTube:     tube,
    watchedTubes: []*Tube{tube},
  }

  return
}
