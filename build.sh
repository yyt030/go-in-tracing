#!/bin/bash

export GOOS=linux
export OSARCH=amd64
versionId=`git describe --always --long`

go build -o consumer.$versionId cmd/consumer/main.go


