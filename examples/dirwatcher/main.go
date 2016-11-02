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

type DirectoryWatcherSource struct {
	directory  string
	pattern    *regexp.Regexp
	operations fsnotify.Op

	oc       conductor.OperatorContext
	instance int
}

type DirectoryWatcherSourceOpts struct {
	directory  string
	pattern    *regexp.Regexp
	operations fsnotify.Op
}

func NewDirectoryWatcherSource(opts DirectoryWatcherSourceOpts) conductor.CreateSpoutProcessorFunc {
	return func() conductor.SpoutProcessor {
		return &DirectoryWatcherSource{
			directory:  opts.directory,
			pattern:    opts.pattern,
			operations: opts.operations,
		}
	}
}

func (s *DirectoryWatcherSource) Setup(ctx context.Context, oc conductor.OperatorContext, instance int) {
	s.oc = oc
	s.instance = instance
}

func (s *DirectoryWatcherSource) Process(ctx context.Context) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		s.oc.Log().Infoln(err)
		os.Exit(1)
	}
	defer watcher.Close()

	err = watcher.Add(s.directory)
	if err != nil {
		s.oc.Log().Infoln(err)
		os.Exit(1)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-watcher.Events:
			if s.pattern.MatchString(path.Base(e.Name)) && s.operations&e.Op != 0 {
				s.oc.Submit(conductor.TupleData{"name": e.Name, "operation": e.Op.String()}, 0)
			} else if s.pattern.MatchString(path.Base(e.Name)) && s.operations&e.Op == 0 && s.oc.NumPorts() > 1 {
				s.oc.Submit(conductor.TupleData{"name": e.Name, "operation": e.Op.String()}, 1)
			}
		case err := <-watcher.Errors:
			s.oc.Log().Infof("%s error: %s", s.oc.Name(), err)
		}
	}
}

func (s *DirectoryWatcherSource) Teardown() {}

type FileReader struct {
	oc       conductor.OperatorContext
	instance int
}

func NewFileReader() conductor.CreateBoltProcessorFunc {
	return func() conductor.BoltProcessor {
		return &FileReader{}
	}
}

func (b *FileReader) Setup(ctx context.Context, oc conductor.OperatorContext, instance int) {
	b.oc = oc
	b.instance = instance
}
func (b *FileReader) Process(ctx context.Context, t conductor.Tuple, port int) {
	name := t.Data["name"].(string)
	operation := t.Data["operation"].(string)

	if operation == "CREATE" || operation == "WRITE" {
		if data, err := ioutil.ReadFile(name); err != nil {
			b.oc.Log().Infof("%s error: %s", b.oc.Name(), err)
		} else {
			b.oc.Submit(conductor.TupleData{"name": name, "operation": operation, "data": data}, 0)
		}
	} else {
		b.oc.Submit(t.Data, 0)
	}
}
func (b *FileReader) Teardown() {}

type LoggerSink struct {
	oc       conductor.OperatorContext
	instance int
}

func NewLoggerSink() conductor.CreateBoltProcessorFunc {
	return func() conductor.BoltProcessor {
		return &LoggerSink{}
	}
}

func (b *LoggerSink) Setup(ctx context.Context, oc conductor.OperatorContext, instance int) {
	b.oc = oc
	b.instance = instance
}
func (b *LoggerSink) Process(ctx context.Context, t conductor.Tuple, port int) {
	b.oc.Log().Infof("%+v %+v", t.Metadata, t.Data)
}
func (b *LoggerSink) Teardown() {}
