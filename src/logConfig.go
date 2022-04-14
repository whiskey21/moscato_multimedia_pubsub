package main

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func MyEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:  "T",
		LevelKey: "L",
		NameKey:  "N",
		//CallerKey:      "C",
		FunctionKey: zapcore.OmitKey,
		MessageKey:  "M",
		//StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

func NewMyLogger() *zap.Logger {
	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:      true,
		Encoding:         "console",
		EncoderConfig:    MyEncoderConfig(),
		OutputPaths:      []string{"stdout", "./test.log"},
		ErrorOutputPaths: []string{"stderr"},
	}
	logger, err := cfg.Build()
	if err != nil {
		//panic(err)
	}
	return logger
}
