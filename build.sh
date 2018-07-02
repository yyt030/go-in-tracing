#!/bin/bash

export GOOS=linux
export OSARCH=amd64

go build cmd/consumer/main.go


