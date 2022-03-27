package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	reg           prometheus.Registerer
	dataProcessed *prometheus.CounterVec
	dataPipeline  *prometheus.HistogramVec
	dataSink      *prometheus.HistogramVec
}

func NewMetrics() *Metrics {
	reg := prometheus.DefaultRegisterer
	factory := promauto.With(reg)

	metrics := &Metrics{
		reg: reg,
		dataProcessed: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "data_processed",
			Help: "Total data Processed",
		}, []string{"build"}),

		dataPipeline: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name: "data_pipeline",
			Help: "Time for processing data, marshalling it to json and writing to Kafka",
		}, []string{"build"}),

		dataSink: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name: "data_sink",
			Help: "Time for processing data, to sink completion",
		}, []string{"build"}),
	}

	metrics.reg.MustRegister(metrics.dataProcessed)
	metrics.reg.MustRegister(metrics.dataPipeline)
	metrics.reg.MustRegister(metrics.dataSink)
	return metrics
}
