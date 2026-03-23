package log

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"refina-transaction/config/env"
	"refina-transaction/internal/utils/data"

	"github.com/sirupsen/logrus"
)

// ApacheStyleFormatter — custom formatter with Apache/Nginx style and color support.
type ApacheStyleFormatter struct {
	NoColors bool
}

// levelColor returns the ANSI color code for a given log level.
// Returns an empty string when colors are disabled.
func (f *ApacheStyleFormatter) levelColor(level logrus.Level) string {
	if f.NoColors {
		return ""
	}
	switch level {
	case logrus.DebugLevel:
		return "\x1b[36m" // Cyan
	case logrus.InfoLevel:
		return "\x1b[32m" // Green
	case logrus.WarnLevel:
		return "\x1b[33m" // Yellow
	case logrus.ErrorLevel:
		return "\x1b[31m" // Red
	case logrus.FatalLevel, logrus.PanicLevel:
		return "\x1b[35m" // Magenta
	case logrus.TraceLevel:
		return "\x1b[37m" // White
	default:
		return "\x1b[0m"
	}
}

const resetColor = "\x1b[0m"

// writeHeader writes the "[timestamp] LEVEL: message" prefix.
func (f *ApacheStyleFormatter) writeHeader(b *bytes.Buffer, entry *logrus.Entry) {
	timestamp := entry.Time.Format("02/Jan/2006:15:04:05 -0700")
	level := strings.ToUpper(entry.Level.String())

	if f.NoColors {
		fmt.Fprintf(b, "[%s] %s: %s", timestamp, level, entry.Message)
		return
	}
	fmt.Fprintf(b, "[%s] %s%s%s: %s",
		timestamp,
		f.levelColor(entry.Level),
		level,
		resetColor,
		entry.Message,
	)
}

// writeFields appends " - key: value, ..." when there are log fields.
func writeFields(b *bytes.Buffer, fields logrus.Fields) {
	if len(fields) == 0 {
		return
	}
	fmt.Fprint(b, " - ")
	first := true
	for key, value := range fields {
		if !first {
			fmt.Fprint(b, ", ")
		}
		fmt.Fprintf(b, "%s: %s", key, formatFieldValue(value))
		first = false
	}
}

// formatFieldValue quotes string values that contain spaces, commas, or equals signs.
func formatFieldValue(value any) string {
	s, ok := value.(string)
	if !ok {
		return fmt.Sprintf("%v", value)
	}
	if strings.ContainsAny(s, " ,=") {
		return fmt.Sprintf(`"%s"`, s)
	}
	return s
}

// Format implements the logrus.Formatter interface.
func (f *ApacheStyleFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b bytes.Buffer
	if entry.Buffer != nil {
		b = *entry.Buffer
	}

	f.writeHeader(&b, entry)
	writeFields(&b, entry.Data)
	b.WriteByte('\n')
	return b.Bytes(), nil
}

var Log *logrus.Logger

func SetupLogger() {
	Log = logrus.New()

	switch env.Cfg.Server.Mode {
	case data.PRODUCTION_MODE:
		setupProductionLogger(Log)
	case data.STAGING_MODE:
		setupStagingLogger(Log)
	default:
		setupDevelopmentLogger(Log)
	}

	Log.Debug(env.Cfg.Server.Mode)
}

func setupProductionLogger(l *logrus.Logger) {
	l.SetLevel(logrus.InfoLevel)
	l.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
			logrus.FieldKeyFunc:  "function",
		},
	})
	file, err := os.OpenFile(".server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		l.Fatal("Failed to open log file:", err)
	}
	l.SetOutput(file)
	l.Debug("Production Log")
}

func setupStagingLogger(l *logrus.Logger) {
	l.SetLevel(logrus.TraceLevel)
	l.SetFormatter(&ApacheStyleFormatter{NoColors: true})
	l.SetOutput(os.Stdout)
	l.Debug("Staging Log")
}

func setupDevelopmentLogger(l *logrus.Logger) {
	l.SetLevel(logrus.TraceLevel)
	l.SetFormatter(&ApacheStyleFormatter{NoColors: false})
	l.SetOutput(os.Stdout)
	l.Debug("Development Log")
}

// ── Helper functions ──

func Info(msg string, fields ...map[string]any) {
	entry := Log.WithFields(logrus.Fields{})
	if len(fields) > 0 {
		entry = Log.WithFields(logrus.Fields(fields[0]))
	}
	entry.Info(msg)
}

func Error(msg string, fields ...map[string]any) {
	entry := Log.WithFields(logrus.Fields{})
	if len(fields) > 0 {
		entry = Log.WithFields(logrus.Fields(fields[0]))
	}
	entry.Error(msg)
}

func Warn(msg string, fields ...map[string]any) {
	entry := Log.WithFields(logrus.Fields{})
	if len(fields) > 0 {
		entry = Log.WithFields(logrus.Fields(fields[0]))
	}
	entry.Warn(msg)
}

func Debug(msg string, fields ...map[string]any) {
	entry := Log.WithFields(logrus.Fields{})
	if len(fields) > 0 {
		entry = Log.WithFields(logrus.Fields(fields[0]))
	}
	entry.Debug(msg)
}

func Fatal(msg string, fields ...map[string]any) {
	entry := Log.WithFields(logrus.Fields{})
	if len(fields) > 0 {
		entry = Log.WithFields(logrus.Fields(fields[0]))
	}
	entry.Fatal(msg)
}

func Trace(msg string, fields ...map[string]any) {
	entry := Log.WithFields(logrus.Fields{})
	if len(fields) > 0 {
		entry = Log.WithFields(logrus.Fields(fields[0]))
	}
	entry.Trace(msg)
}