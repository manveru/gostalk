package main

import (
	"github.com/manveru/gostalk"
)

func main() {
	// buffer 1, the channel is only useful for testing and embedding.
	running := make(chan bool, 1)
	gostalk.Start("127.0.0.1:40400", running)
}
