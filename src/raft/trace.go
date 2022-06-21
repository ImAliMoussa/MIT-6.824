package raft

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
)

// Debugging
const Debug = 0

func trace(a ...interface{}) {
	if Debug == 1 && os.Getenv("LOG") == "1" {
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime | log.Lshortfile))
		pc, filename, line, _ := runtime.Caller(1)
		filenameTokens := strings.Split(filename, "/")
		filename = filenameTokens[len(filenameTokens)-1]
		funcName := runtime.FuncForPC(pc).Name()
		s := fmt.Sprintln(a...)
		log.Printf("%s[%s:%d]\n%s\n", funcName, filename, line, s)
	}
}
