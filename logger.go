package conductor

import (
	"io"
	"log"
)

type InfoDebugLogger interface {
	Info(...interface{})
	Infof(string, ...interface{})
	Infoln(...interface{})

	Debug(...interface{})
	Debugf(string, ...interface{})
	Debugln(...interface{})
	SetDebug(bool)
}

type Logger struct {
	log.Logger
	debug bool
}

func NewLogger(out io.Writer, prefix string, flags int) *Logger {
	l := &Logger{}
	l.debug = false
	l.SetOutput(out)
	l.SetPrefix(prefix)
	l.SetFlags(flags)
	return l
}

func (l *Logger) Info(args ...interface{}) {
	l.Print(append([]interface{}{"[INFO] "}, args...)...)
}
func (l *Logger) Infof(format string, args ...interface{}) {
	l.Printf("[INFO] "+format, args...)
}
func (l *Logger) Infoln(args ...interface{}) {
	l.Println(append([]interface{}{"[INFO]"}, args...)...)
}

func (l *Logger) Debug(args ...interface{}) {
	if l.debug {
		l.Print(append([]interface{}{"[DEBUG] "}, args...)...)
	}
}
func (l *Logger) Debugf(format string, args ...interface{}) {
	if l.debug {
		l.Printf("[DEBUG] "+format, args...)
	}
}
func (l *Logger) Debugln(args ...interface{}) {
	if l.debug {
		l.Println(append([]interface{}{"[DEBUG]"}, args...)...)
	}
}
func (l *Logger) SetDebug(debug bool) {
	l.debug = debug
}
