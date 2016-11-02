package main

import (
	"context"
	"log"
	"regexp"
	"time"

	"github.com/fsnotify/fsnotify"

	"flag"

	"github.com/joelanford/conductor"
	"github.com/joelanford/conductor/operators"
)

func main() {
	dir := flag.String("directory", "", "Directory to watch")
	pattern := flag.String("pattern", ".*", "Regex pattern to match")
	flag.Parse()

	if *dir == "" {
		log.Fatal("-directory flag is required")
	}

	regex, err := regexp.Compile(*pattern)
	if err != nil {
		log.Fatal("invalid -pattern value")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	t := conductor.NewTopology("my-topology")

	t.AddSpout("dirwatcher", operators.NewDirectoryWatcher(*dir, regex, fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename), 1).
		Produces("filechanges")
	t.AddBolt("filereader", operators.NewFileReader(), 10).
		Consumes("filechanges", conductor.PartitionRoundRobin(), 1000).
		Produces("files")
	t.AddBolt("printer", operators.NewTupleLogger(), 2).
		Consumes("filechanges", conductor.PartitionRoundRobin(), 1000).
		Consumes("files", conductor.PartitionRoundRobin(), 1000)

	t.Run(ctx)
	cancel()
}
