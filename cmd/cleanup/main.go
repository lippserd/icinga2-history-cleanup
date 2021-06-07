package main

import (
	"context"
	"fmt"
	"github.com/lippserd/icinga2-history-cleanup/internal/command"
	"github.com/lippserd/icinga2-history-cleanup/pkg/contracts"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const (
	ExitSuccess = 0
	ExitFailure = 1
)

// errorHandling recovers from panic, captures the panic value, prints stack traces, and exits with 1.
func errorHandling() {
	if r := recover(); r != nil {
		stderr := log.New(os.Stderr, "", 0)

		stderr.Println(r)

		type stackTracer interface {
			StackTrace() errors.StackTrace
		}

		st, ok := r.(stackTracer)
		if !ok {
			// Capture the runtime stack, although it might not be useful anyway.
			st = errors.New(fmt.Sprintf("%+v", r)).(stackTracer)
		}

		stderr.Println()
		for _, f := range st.StackTrace() {
			stderr.Printf("%+s:%d\n", f, f)
		}

		os.Exit(ExitFailure)
	}
}

func main() {
	os.Exit(run())
}

func run() int {
	defer errorHandling()

	cmd, err := command.New()
	if err != nil {
		panic(err)
	}

	logger := cmd.Logger
	defer logger.Sync()

	logger.Info("Starting Icinga 2 history cleanup")

	ido, err := cmd.Database()
	if err != nil {
		panic(err)
	}
	defer ido.Close()
	{
		logger.Info("Connecting to database")
		err := ido.Ping()
		if err != nil {
			panic(errors.Wrap(err, "can't connect to database"))
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	ctx := context.Background()
	cleanupCtx, cancelCleanupCtx := context.WithCancel(ctx)
	done := make(chan bool, 1)

	g, ctx := errgroup.WithContext(cleanupCtx)
	for _, table := range contracts.Tables {
		table := table
		g.Go(func() error {
			return ido.Cleanup(ctx, &table)
		})
	}

	go func() {
		select {
		case <-ctx.Done():
			// Noop
		case s := <-sig:
			logger.Infow("Exiting due to signal", zap.String("signal", s.String()))
			cancelCleanupCtx()
			done <- true
		}
	}()

	go func() {
		if err := g.Wait(); err != nil {
			panic(err)
		}
		logger.Info("Finished cleanup")
		done <- true
	}()

	<-done

	return ExitSuccess
}
