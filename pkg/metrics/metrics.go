package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	Reg           prometheus.Registerer
	DataProcessed *prometheus.CounterVec
	DataPipeline  *prometheus.HistogramVec
	DataSink      *prometheus.HistogramVec
}

func NewMetrics() *Metrics {
	reg := prometheus.DefaultRegisterer
	factory := promauto.With(reg)

	metrics := &Metrics{
		Reg: reg,
		DataProcessed: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "data_processed",
			Help: "Total data Processed",
		}, []string{"build"}),

		DataPipeline: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name: "data_pipeline",
			Help: "Time for processing data, marshalling it to json and writing to Kafka",
		}, []string{"build"}),

		DataSink: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name: "data_sink",
			Help: "Time for processing data, to sink completion",
		}, []string{"build"}),
	}

	/*
		metrics.Reg.MustRegister(metrics.DataProcessed)
		metrics.Reg.MustRegister(metrics.DataPipeline)
		metrics.Reg.MustRegister(metrics.DataSink)
	*/
	return metrics
}
