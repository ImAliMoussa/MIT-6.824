package raft

import (
	"fmt"
	"log"
	"runtime"
)

// Debugging
const Debug = 1

func trace(a ...interface{}) {
	if Debug == 1 {
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
