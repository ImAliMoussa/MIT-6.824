package kvraft

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	PUT            = "PUT"
	APPEND         = "APPEND"
	GET            = "GET"
	ClerkTimeout   = 250 * time.Millisecond
)

type Err string

func (ck *Clerk) Trace(a ...interface{}) {
	// Debugging
	const Debug = 1

	if Debug == 1 && os.Getenv("LOG") == "1" {
		s := fmt.Sprintln(a...)

		pc := make([]uintptr, 10)
		runtime.Callers(2, pc)
		f := runtime.FuncForPC(pc[0])
		_, line := f.FileLine(pc[0])

		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime | log.Lshortfile))
		log.Printf("%s@%d\nClerk: %s\n", f.Name(), line, s)
	}
}

func (kv *KVServer) Trace(a ...interface{}) {
	// Debugging
	const Debug = 1

	if Debug == 1 && os.Getenv("LOG") == "1" {
		s := fmt.Sprintln(a...)

		pc := make([]uintptr, 10)
		runtime.Callers(2, pc)
		f := runtime.FuncForPC(pc[0])
		_, line := f.FileLine(pc[0])

		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime | log.Lshortfile))
		log.Printf("%s@%d\nKv(%d): %s\n", f.Name(), line, kv.me, s)
	}
}

func PP(i interface{}) string {
	out, _ := json.Marshal(i)
	return string(out)
}
