package raft

import (
	"fmt"
	"log"
	"os"
	"runtime"
)

// Debugging
const Debug = 1

func trace(a ...interface{}) {
	if Debug == 1 && os.Getenv("LOG") == "1" {
		s := fmt.Sprintln(a...)

		pc := make([]uintptr, 10)
		runtime.Callers(2, pc)
		f := runtime.FuncForPC(pc[0])
		_, line := f.FileLine(pc[0])

		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime | log.Lshortfile))
		// yellow := "\033[33m"
		// reset := "\033[0m"
		// log.Printf("%s%s@%d%s\n%s\n", yellow, f.Name(), line, reset, s)
		log.Printf("%s@%d\n%s\n", f.Name(), line, s)
	}
}
