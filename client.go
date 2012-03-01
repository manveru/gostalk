package gostalker

import (
  "bufio"
  "net"
)

type Client struct {
  conn         net.Conn
  reader       Reader
  usedTube     *Tube
  watchedTubes []*Tube
}

func newClient(conn net.Conn, tube *Tube) (client *Client) {
  client = &Client{
    conn:         conn,
    reader:       bufio.NewReader(conn),
    usedTube:     tube,
    watchedTubes: []*Tube{tube},
  }

  return
}
