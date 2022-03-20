package kvraft

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
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
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime | log.Lshortfile))
		pc, filename, line, _ := runtime.Caller(1)
		filenameTokens := strings.Split(filename, "/")
		filename = filenameTokens[len(filenameTokens)-1]
		funcName := runtime.FuncForPC(pc).Name()
		s := fmt.Sprintln(a...)
		log.Printf("%s[%s:%d]\nClerk: %s\n", funcName, filename, line, s)
	}
}

func (kv *KVServer) Trace(a ...interface{}) {
	// Debugging
	const Debug = 1

	if Debug == 1 && os.Getenv("LOG") == "1" {
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime | log.Lshortfile))
		pc, filename, line, _ := runtime.Caller(1)
		filenameTokens := strings.Split(filename, "/")
		filename = filenameTokens[len(filenameTokens)-1]
		funcName := runtime.FuncForPC(pc).Name()
		s := fmt.Sprintln(a...)
		log.Printf("%s[%s:%d]\nKV(%d): %s\n", funcName, filename, line, kv.me, s)
	}
}

func PP(i interface{}) string {
	out, _ := json.Marshal(i)
	return string(out)
}
