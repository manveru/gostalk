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
  gostalker.Start()
}
