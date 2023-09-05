package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type LogRole string
type LogTopic string

const (
	rError             LogTopic = "[ERROR] "
	rAppendAccept      LogTopic = "[Append Accepted] "
	rAppendReject      LogTopic = "[Append Rejected] "
	rAppendSend        LogTopic = "[Append Send] "
	rCommit            LogTopic = "[Commit] "
	rLeader            LogTopic = "[Become Leader] "
	rSnapshotCreate    LogTopic = "[Create Snapshot] "
	rSnapshotInstalled LogTopic = "[Install Snapshot]"
	rSnapshotStart     LogTopic = "[Start Snapshot] "
	rSnapshotAccept    LogTopic = "[Accept Snapshot] "
	rStartElection     LogTopic = "[Start Election] "
	rVoteGranted       LogTopic = "[Vote Granted] "
	rVoteRejected      LogTopic = "[Vote Rejected] "
)

var start time.Time
var debugVerbosity int
var mu sync.Mutex

func InitLog() {
	start = time.Now()
	mu.Lock()
	debugVerbosity = getVerbosity()
	mu.Unlock()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func RaftDebug(topic LogTopic, format string, a ...interface{}) {
	mu.Lock()
	if debugVerbosity >= 1 {
		time := time.Since(start).Microseconds()
		time /= 100
		pattern := fmt.Sprintf("%06d %v", time, string(topic))
		format = pattern + format
		log.Printf(format, a...)
	}
	mu.Unlock()
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	if v != "" {
		level, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)

		}
		return level
	}
	return 0
}
