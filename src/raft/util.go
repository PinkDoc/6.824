package raft

import "log"

// Debugging
const Debug = false

func Min(a int, b int) int {
	if a > b {
		return b
	}

	return a
}

func Max(a int, b int) int {
	if a > b {
		return a
	}

	return b
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
