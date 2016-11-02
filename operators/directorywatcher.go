package operators

import (
	"context"
	"os"
	"path"
	"regexp"

	"github.com/fsnotify/fsnotify"
	"github.com/joelanford/conductor"
)

type DirectoryWatcher struct {
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

func NewDirectoryWatcher(directory string, pattern *regexp.Regexp, operations fsnotify.Op) conductor.CreateSpoutProcessorFunc {
	return func() conductor.SpoutProcessor {
		return &DirectoryWatcher{
			directory:  directory,
			pattern:    pattern,
			operations: operations,
		}
	}
}

func (s *DirectoryWatcher) Setup(ctx context.Context, oc conductor.OperatorContext, instance int) {
	s.oc = oc
	s.instance = instance
}

func (s *DirectoryWatcher) Process(ctx context.Context) {
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

func (s *DirectoryWatcher) Teardown() {}
