package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 下面是blog上的log相关组件
type LogTopic string

const (
	DClient  LogTopic = "CLNT"
	dCommit  LogTopic = "CMIT"
	dDrop    LogTopic = "DROP"
	dError   LogTopic = "ERRO"
	DInfo    LogTopic = "INFO"
	dLeader  LogTopic = "LEAD"
	dLog     LogTopic = "LOG1"
	dLog2    LogTopic = "LOG2"
	dPersist LogTopic = "PERS"
	dSnap    LogTopic = "SNAP"
	dTerm    LogTopic = "TERM"
	dTest    LogTopic = "TEST"
	dTimer   LogTopic = "TIMR"
	dTrace   LogTopic = "TRCE"
	dVote    LogTopic = "VOTE"
	DWarn    LogTopic = "WARN"
	DServer  LogTopic = "SERV"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func MyDebug(topic LogTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func Max(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
