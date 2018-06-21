#!/bin/bash

export GOOS=linux
export OSARCH=amd64

go build cmd/svc/main.go


