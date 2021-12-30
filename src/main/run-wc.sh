#!/bin/sh

# Remove any temporary files and any .so files from previous build
rm mr-out* *.so

set -e

go build -race -buildmode=plugin ../mrapps/wc.go

go run -race mrcoordinator.go pg-*.txt
