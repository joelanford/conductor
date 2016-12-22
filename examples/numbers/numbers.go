package main

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"flag"

	"github.com/braintree/manners"
	"github.com/joelanford/streams"
	"github.com/joelanford/streams/operators"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	graphFile := flag.String("g", "", "Create GraphViz specified file instead of running the topology")
	flag.Parse()

	even := func(ctx context.Context, oc *streams.OperatorContext) {
		i := 0
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "speed",
			Name:      "numbers_generated_even_total",
			Help:      "Counter for the total number of even numbers generated",
		}, []string{"operator", "instance"})
		oc.RegisterMetric(counter)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if i > 9999999999 {
					return
				}
				counter.WithLabelValues(oc.Name(), strconv.Itoa(oc.Instance())).Inc()
				oc.Submit(&streams.Tuple{Data: streams.TupleData{"i": i}}, 0)
				i = i + 2

			}
		}
	}

	odd := func(ctx context.Context, oc *streams.OperatorContext) {
		i := 1
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "speed",
			Name:      "numbers_generated_odd_total",
			Help:      "Counter for the total number of even numbers generated",
		}, []string{"operator", "instance"})
		oc.RegisterMetric(counter)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if i > 9999999999 {
					return
				}
				counter.WithLabelValues(oc.Name(), strconv.Itoa(oc.Instance())).Inc()
				oc.Submit(&streams.Tuple{Data: streams.TupleData{"i": i}}, 0)
				i = i + 2
			}
		}
	}

	filter := func(t *streams.Tuple) bool {
		return t.Data["i"].(int)%2 == 0
	}

	printer := func(oc *streams.OperatorContext, t *streams.Tuple, port int) {
		oc.Log().Infoln(t.Metadata, t.Data)
	}

	t := streams.NewTopology("speed")

	evenNumbers := t.AddStream("evenNumbers")
	oddNumbers := t.AddStream("oddNumbers")
	filtered := t.AddStream("filtered")

	t.AddSpout("evenSource", operators.NewCustomSpout(even), 1).Produces(evenNumbers)
	t.AddSpout("oddSource", operators.NewCustomSpout(odd), 1).Produces(oddNumbers)
	//t.AddBolt("unfilteredPrinter", operators.NewCustom(printer), 1).Consumes(evenNumbers, 0).Consumes(oddNumbers, 0)
	t.AddBolt("filter", operators.NewFilter(filter), 10).Consumes(evenNumbers, 1).Consumes(oddNumbers, 1).Produces(filtered)
	t.AddBolt("filteredPrinter", operators.NewCustom(printer), 1).Consumes(filtered, 0)
	//t.AddBolt("allPrinter", operators.NewCustom(printer), 1).Consumes(evenNumbers, 1).Consumes(oddNumbers, 1).Consumes(filtered, 1)

	if *graphFile != "" {
		t.CreateGraphFile(*graphFile)
	} else {
		var wg sync.WaitGroup
		prometheus.MustRegister(prometheus.NewProcessCollector(os.Getpid(), "streams"))
		prometheus.MustRegister(t.NewPrometheusCollector())

		r := http.NewServeMux()
		r.Handle("/metrics", prometheus.Handler())

		wg.Add(1)
		go func() {
			manners.ListenAndServe(":9100", r)
			wg.Done()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*600)
		t.Run(ctx)

		manners.Close()
		wg.Wait()
		cancel()
	}
}
