package main

type silentLogger struct{}

func (l silentLogger) Debug(v ...interface{})                 {}
func (l silentLogger) Debugf(format string, v ...interface{}) {}

func (l silentLogger) Info(v ...interface{})                 {}
func (l silentLogger) Infof(format string, v ...interface{}) {}

func (l silentLogger) Warn(v ...interface{})                 {}
func (l silentLogger) Warnf(format string, v ...interface{}) {}

func (l silentLogger) Warning(v ...interface{})                 {}
func (l silentLogger) Warningf(format string, v ...interface{}) {}

func (l silentLogger) Error(v ...interface{})                 {}
func (l silentLogger) Errorf(format string, v ...interface{}) {}

func (l silentLogger) Fatal(v ...interface{})                 {}
func (l silentLogger) Fatalf(format string, v ...interface{}) {}

func (l silentLogger) Panic(v ...interface{})                 {}
func (l silentLogger) Panicf(format string, v ...interface{}) {}
