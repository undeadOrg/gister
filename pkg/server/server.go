package server

import (
	"context"
	"gister/pkg/metrics"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	clientId      = "gister"
	consumerGroup = "gister"
)

// Server
type Server interface {
	Brokers([]string) Server
	InTopic(string) Server
	OutTopic(string) Server
	Debug(bool) Server
	WorkerCount(int) Server
	Run(context.Context, context.CancelFunc, *sync.WaitGroup)
}

type server struct {
	log         *log.Entry
	metrics     *metrics.Metrics
	brokers     []string
	inTopic     string
	outTopic    string
	workerCount int
	debug       bool
}

// NewServer - Create Server instance with Logger and Kafka Client
func NewServer(logger *log.Entry) *server {
	/*
		if debug {
			log.SetFormatter(&log.TextFormatter{})
			log.SetLevel(log.DebugLevel)
		} else {
			log.SetFormatter(&log.JSONFormatter{})
		}

		logger := log.WithFields(log.Fields{
			"service": "gister",
			"build":   buildNum,
		})
	*/

	s := &server{
		workerCount: 1,
		log:         logger,
		//metrics: metrics.NewMetrics(),
	}

	return s
}

// Brokers sets kafka brokers
func (s *server) Brokers(b []string) *server {
	s.brokers = b
	return s
}

// InTopic set the ingest topic
func (s *server) InTopic(t string) *server {
	s.inTopic = t
	return s
}

// OutTopic set the output topic
func (s *server) OutTopic(t string) *server {
	s.outTopic = t
	return s
}

// WorkerCount set the number of output workers
func (s *server) WorkerCount(w int) *server {
	s.workerCount = w
	return s
}

// Debug enable
func (s *server) Debug(d bool) *server {
	s.debug = d
	return s
}

// Run
func (s *server) Run(ctx context.Context, ctxCancel context.CancelFunc) {
	// Consumer from Kafka
	// Pipe  filter thread
	// Publish back to new Kafka
	s.log.Info("Starting Up Server....")

	// Setup WaitGroup
	wg := &sync.WaitGroup{}

	s.log.WithFields(log.Fields{
		"brokers": s.brokers,
	}).Info("Connecting To Brokers")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(s.brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(s.inTopic),
		kgo.ClientID(clientId),
	)
	if err != nil {
		s.log.Error("Error setting up consumer client: %v", err)
	}

	pipelineChan := make(chan *kgo.Record, s.workerCount)
	producerChan := make(chan interface{}, s.workerCount)

	wg.Add(3)
	s.log.Info("Starting Up Processing Threads")
	go s.consume(ctx, wg, client, pipelineChan)
	go s.pipeline(ctx, wg, pipelineChan, producerChan)
	go s.sink(ctx, wg, client, producerChan)

	wg.Wait()
	// Close Kafka client
	client.Close()
	s.log.Info("Server Shut Down")
}

func (s *server) consume(ctx context.Context, wg *sync.WaitGroup, client *kgo.Client, output chan<- *kgo.Record) {
	defer wg.Done()
	for {
		select {
		default:
			fetches := client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}

			fetches.EachError(func(t string, p int32, err error) {
				s.log.Error("fetch err topic %s partition %d: %v", t, p, err)
			})

			// Iterate through messages
			fetches.EachRecord(func(r *kgo.Record) {
				output <- r
			})
		case <-ctx.Done():
			client.LeaveGroup()
			s.log.Info("Closing Consumer")
			return
		}
	}
}

func (s *server) pipeline(ctx context.Context, wg *sync.WaitGroup, input <-chan *kgo.Record, output chan<- interface{}) {
	defer wg.Done()
	for {
		select {
		case m := <-input:
			s.log.Info("I can have message")
			output <- m
		case <-ctx.Done():
			s.log.Info("Exiting Pipeline")
			return
		}
	}
}

func (s *server) sink(ctx context.Context, wg *sync.WaitGroup, client *kgo.Client, input <-chan interface{}) {
	defer wg.Done()
	for {
		select {
		case <-input:
			s.log.Info(".")
		case <-ctx.Done():
			s.log.Info("Exiting Sink")
			return
		}
		/*
			record := &kgo.Record{Topic: s.outTopic, Value: []byte("bar")}
			client.Produce(ctx, record, func(_ *kgo.Record, err error) {
				if err != nil {
					fmt.Printf("record had a produce error: %v\n", err)
				}

			})
		*/
	}
}
