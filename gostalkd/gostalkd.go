package main

import (
  "fmt"
  "gostalker"
  "runtime"
)

func main() {
  numCPU := runtime.NumCPU()
  fmt.Printf("Setting GOMAXPROCS to %d\n", numCPU)
  runtime.GOMAXPROCS(numCPU)
  running := make(chan bool, 1)
  gostalker.Start("127.0.0.1:40400", running)
}
