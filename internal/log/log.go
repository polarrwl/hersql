package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.SugaredLogger

func init() {
	logger = zap.New(zapcore.NewCore(getEncoder(), getLogWriter(), zapcore.DebugLevel), zap.AddCaller()).Sugar()
}

func Debug(args ...interface{}) {
	logger.Debug(args...)
}

func Info(args ...interface{}) {
	logger.Info(args...)
}

func Warn(args ...interface{}) {
	logger.Warn(args...)
}

func Error(args ...interface{}) {
	logger.Warn(args...)
}

func DPanic(args ...interface{}) {
	logger.Warn(args...)
}
func Panic(args ...interface{}) {
	logger.Panic(args...)
}
func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	// 修改时间编码器
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	// 在日志文件中使用大写字母记录日志级别
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	// NewConsoleEncoder 打印更符合人们观察的方式
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func getLogWriter() zapcore.WriteSyncer {
	return zapcore.AddSync(os.Stdout)
}
