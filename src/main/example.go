package main

import (
	"fmt"
	"time"
)

func handle(t *time.Timer) {
	fmt.Println("Hello")
	t.Stop()
	t.Reset(1 * time.Second)
}

func main() {
	t := time.NewTimer(1 * time.Second)

	for {
		select {
		case <-t.C:
			handle(t)
		}
	}

}
