package main

import (
	"io"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

func testLogger() *roleLogger {
	logger, err := servicelog.New(servicelog.Config{Writer: io.Discard, Level: servicelog.LevelInfo, Role: "test"})
	if err != nil {
		panic(err)
	}
	return logger
}
