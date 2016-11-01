package main

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path"
	"regexp"
	"time"

	"github.com/fsnotify/fsnotify"

	"flag"

	"github.com/joelanford/conductor"
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

	opts := DirectoryWatcherSourceOpts{
		directory:  *dir,
		pattern:    regex,
		operations: fsnotify.Create | fsnotify.Write | fsnotify.Remove | fsnotify.Rename,
	}
	t.AddSpout("dirwatcher", NewDirectoryWatcherSource(opts), 1).
		Produces("filechanges")
	t.AddBolt("filereader", NewFileReader(), 10).
		Consumes("filechanges", conductor.PartitionRoundRobin(), 1000).
		Produces("files")
	t.AddBolt("printer", NewLoggerSink(), 2).
		Consumes("filechanges", conductor.PartitionRoundRobin(), 1000).
		Consumes("files", conductor.PartitionRoundRobin(), 1000)

	t.Run(ctx)
	cancel()
}

type DirectoryWatcherSourceOpts struct {
	directory  string
	pattern    *regexp.Regexp
	operations fsnotify.Op
}

func NewDirectoryWatcherSource(opts DirectoryWatcherSourceOpts) conductor.ProcessFunc {
	return func(ctx context.Context, opCtx conductor.OperatorContext) {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			opCtx.Log().Infoln(err)
			os.Exit(1)
		}
		defer watcher.Close()

		err = watcher.Add(opts.directory)
		if err != nil {
			opCtx.Log().Infoln(err)
			os.Exit(1)
		}
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-watcher.Events:
				if opts.pattern.MatchString(path.Base(e.Name)) && opts.operations&e.Op != 0 {
					opCtx.Submit(conductor.TupleData{"name": e.Name, "operation": e.Op.String()}, 0)
				} else if opts.pattern.MatchString(path.Base(e.Name)) && opts.operations&e.Op == 0 && opCtx.NumPorts() > 1 {
					opCtx.Submit(conductor.TupleData{"name": e.Name, "operation": e.Op.String()}, 1)
				}
			case err := <-watcher.Errors:
				opCtx.Log().Infof("%s error: %s", opCtx.Name(), err)
			}
		}
	}
}

func NewFileReader() conductor.ProcessTupleFunc {
	return func(ctx context.Context, opCtx conductor.OperatorContext, t conductor.Tuple, port int) {
		name := t.Data["name"].(string)
		operation := t.Data["operation"].(string)

		if operation == "CREATE" || operation == "WRITE" {
			if data, err := ioutil.ReadFile(name); err != nil {
				opCtx.Log().Infof("%s error: %s", opCtx.Name(), err)
			} else {
				opCtx.Submit(conductor.TupleData{"name": name, "operation": operation, "data": data}, 0)
			}
		} else {
			opCtx.Submit(t.Data, 0)
		}
	}
}

func NewLoggerSink() conductor.ProcessTupleFunc {
	return func(ctx context.Context, opCtx conductor.OperatorContext, t conductor.Tuple, port int) {
		opCtx.Log().Infof("%+v %+v\n", t.Metadata, t.Data)
	}
}
