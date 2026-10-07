package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Fprintf(os.Stdout, "usage: %s [webui-local|webui-local-bootstrap|wecom-local|wecom-ha-entry]\n", os.Args[0])
		fmt.Fprintln(os.Stdout, "Runs the durable conversation and failover runtime.")
		return
	}
	role := "webui-local"
	if len(os.Args) > 1 {
		role = os.Args[1]
	}
	logger, err := servicelog.NewFromEnv(os.Getenv, os.Stderr, role)
	if err != nil {
		fmt.Fprintf(os.Stderr, "trpc-agent-service logging configuration rejected: %v\n", err)
		os.Exit(2)
	}
	processContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runRole(processContext, os.Getenv, logger, role); err != nil {
		logger.Printf("trpc-agent-service stopped: %v", err)
		os.Exit(1)
	}
}

type roleLogger = servicelog.Logger

func run(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	return runRole(parent, getenv, logger, "webui-local")
}

func runRole(parent context.Context, getenv func(string) string, logger *roleLogger, role string) error {
	switch role {
	case "webui-local", "wecom-local":
		return runWebUILocalRole(parent, getenv, logger)
	case "webui-local-bootstrap":
		return runWebUILocalBootstrap(parent, getenv, logger)
	case "wecom-ha-entry":
		return runWeComHAEntryRole(parent, getenv, logger)
	default:
		return errors.New("unsupported service role")
	}
}
