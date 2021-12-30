#!/bin/sh

tmux \
    new-session  'go run -race mrworker.go wc.so' \; \
    split-window 'go run -race mrworker.go wc.so' \; \

