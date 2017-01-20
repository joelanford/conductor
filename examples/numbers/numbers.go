package main

import (
	"context"
	"net/http"
	"net/http/pprof"
	"os"
	"sync"
	"time"

	"flag"

	"github.com/braintree/manners"
	"github.com/joelanford/streams"
	"github.com/joelanford/streams/operators"
	"github.com/prometheus/client_golang/prometheus"
)

func numProducer(start, increment int) operators.CustomSpoutFunc {
	return func(ctx context.Context, oc *streams.OperatorContext) {
		ticker := time.NewTicker(500 * time.Millisecond)
		delta := 0.0
		i := start
		incr := increment
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "numbers",
			Name:      "numbers_generated_total",
			Help:      "Counter for the total number of numbers generated",
		}, []string{"operator", "instance"})
		oc.RegisterMetric(counter)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				delta++
				select {
				case <-ticker.C:
					counter.WithLabelValues(oc.Name(), oc.InstanceString()).Add(delta)
					delta = 0.0
				default:
				}
				oc.Submit(&streams.Tuple{Data: streams.TupleData{"i": i}}, 0)
				// t := streams.CreateTuple()
				// t.Data["i"] = i
				// oc.Submit(t, 0)
				i += incr
			}
		}
	}
}

func filter(t *streams.Tuple) bool {
	return t.Data["i"].(int)%2 == 0
}

func sink(oc *streams.OperatorContext, t *streams.Tuple, port int) {
	//streams.ReturnTuple(t)
}

func main() {
	graphFile := flag.String("g", "", "Create GraphViz specified file instead of running the topology")
	flag.Parse()

	t := streams.NewTopology("numbers")
	filtered := t.AddStream("filtered")

	evenNumbers := t.AddStream("evenNumbers")
	oddNumbers := t.AddStream("oddNumbers")
	t.AddSpout("evenSource", operators.NewCustomSpout(numProducer(0, 2)), 1).Produces(evenNumbers)
	t.AddSpout("oddSource", operators.NewCustomSpout(numProducer(1, 2)), 1).Produces(oddNumbers)
	t.AddBolt("filter", operators.NewFilter(filter), 1).Consumes(evenNumbers, 100).Consumes(oddNumbers, 100).Produces(filtered)

	// numbers := t.AddStream("numbers")
	// t.AddSpout("numberSource", operators.NewCustomSpout(numProducerBetter()), 2).Produces(numbers)
	// t.AddBolt("filter", operators.NewFilter(filter), 1).Consumes(numbers, 100).Produces(filtered)

	t.AddBolt("sink", operators.NewCustom(sink), 1).Consumes(filtered, 1)

	if *graphFile != "" {
		t.CreateGraphFile(*graphFile)
	} else {
		var wg sync.WaitGroup
		prometheus.MustRegister(prometheus.NewProcessCollector(os.Getpid(), "streams"))
		prometheus.MustRegister(t)

		r := http.NewServeMux()
		r.Handle("/metrics", prometheus.Handler())
		//r.HandleFunc("/debug/pprof/", pprof.Index)
		r.HandleFunc("/debug/pprof/profile", pprof.Profile)

		wg.Add(1)
		go func() {
			manners.ListenAndServe(":9100", r)
			wg.Done()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour*24)
		t.Run(ctx)

		manners.Close()
		wg.Wait()
		cancel()
	}
}
