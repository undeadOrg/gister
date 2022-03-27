package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"gister/pkg/server"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	port     string
	buildNum string
)

const (
	serviceName = "gister"
)

func init() {
	buildNum = "dev"
}

func main() {
	flag.StringVar(&port, "port", ":5000", "Port")
	brokers := flag.String("brokers", "127.0.0.1:9092", "Comma separated list of Kafka brokers to connect too")
	topicSrc := flag.String("topic-src", "feedster-sink", "Kafka Topic to write too")
	topicDst := flag.String("topic-dst", "gister-output", "Kafka Topic to write transforms too")
	workerCount := flag.Int("workers", 1, "Number of worker threads")
	debug := flag.Bool("debug", false, "Debug Logging")

	flag.Parse()

	// Setup Connection timeouts
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Logger
	if *debug {
		log.SetFormatter(&log.TextFormatter{})
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetFormatter(&log.JSONFormatter{})
	}

	logger := log.WithFields(log.Fields{
		"service": serviceName,
		"build":   buildNum,
	})

	done := make(chan bool)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	// Setup Server
	server := server.NewServer(logger, buildNum)
	server.Brokers(strings.Split(*brokers, ","))
	server.InTopic(*topicSrc)
	server.OutTopic(*topicDst)
	server.WorkerCount(*workerCount)
	server.Debug(*debug)

	go server.Run(ctx, cancel)

	// Start HTTP Server for metrics/status
	router := mux.NewRouter()
	router.HandleFunc("/", handler)
	router.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         port,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	go func() {
		<-quit
		logger.Println("Server is shutting down...")

		defer cancel()

		srv.SetKeepAlivesEnabled(false)
		if err := srv.Shutdown(ctx); err != nil {
			logger.Fatalf("Could not gracefully shutdown the server: %v\n", err)
		}
		close(done)
	}()

	logger.Info("Starting Web Server on port: ", port)
	logger.Fatal(srv.ListenAndServe())
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "HeloWorld")
}
