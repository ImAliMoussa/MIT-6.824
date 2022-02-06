package raft

import (
	"log"
	"fmt"
	"runtime"
)

// Debugging
const Debug = true

func trace(a ...interface{}) {
	if Debug {
		yellow := "\033[33m"
		reset := "\033[0m"
		pc := make([]uintptr, 10)
		runtime.Callers(2, pc)
		f := runtime.FuncForPC(pc[0])
		_, line := f.FileLine(pc[0])
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime | log.Lshortfile))
		s := fmt.Sprintln(a...)
		log.Printf("%s%s@%d%s\n%s\n", yellow, f.Name(), line, reset, s)
	}
}
