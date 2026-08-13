package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"own-kafka/internal/app"
)


const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)


func main() {
	log := setupLogger(envLocal)

	application, err := app.New(log, 9092, "../tmp/kraft-combined-logs")
	if err!=nil {
		panic(err)
	}

	go func() {
		application.TCPCServer.Run()
	}()

	// Graceful shutdown

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	<-stop

	application.TCPCServer.Stop()
	log.Info("Gracefully stopped")
}

func setupLogger(env string) *slog.Logger {
	var log *slog.Logger

	switch env {
	case envLocal:
		log = slog.New(
			slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envDev:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envProd:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		)
	}

	return log
}

