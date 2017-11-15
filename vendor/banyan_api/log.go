package banyan_api

import (
    "fmt"
    "os"
    "log"
)

const (
    LevelDebug = iota
    LevelInfo
    LevelWarn
    LevelError
    LevelFatal
)

var logger = log.New(os.Stderr, "", log.LstdFlags)
var logLevel =  LevelInfo

func LogInit(level int, filename string) {
    logLevel = level
    fp, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
    if err == nil {
        logger = log.New(fp, "", log.LstdFlags|log.Lshortfile)
    }
}

func StringToLogLevel(level string) int {
    switch level {
    case "debug":
        return LevelDebug
    case "info":
        return LevelInfo
    case "warn":
        return LevelWarn
    case "error":
        return LevelError
    case "fatal":
        return LevelFatal
    }
    return LevelFatal+1
}

func Debug(s string, args ...interface{}) {
    if (logLevel <= LevelDebug) {
        s = "debug " + s
        logger.Output(2, fmt.Sprintf(s, args...))
    }
}

func Info(s string, args ...interface{}) {
    if (logLevel <= LevelInfo) {
        s = "info " + s
        logger.Output(2, fmt.Sprintf(s, args...))
    }
}

func Warn(s string, args ...interface{}) {
    if (logLevel <= LevelWarn) {
        s = "warn " + s
        logger.Output(2, fmt.Sprintf(s, args...))
    }
}

func Error(s string, args ...interface{}) {
    if (logLevel <= LevelError) {
        s = "error " + s
        logger.Output(2, fmt.Sprintf(s, args...))
    }
}

func Fatal(s string, args ...interface{}) {
    if (logLevel <= LevelFatal) {
        s = "fatal " + s
        logger.Output(2, fmt.Sprintf(s, args...))
    }
}
