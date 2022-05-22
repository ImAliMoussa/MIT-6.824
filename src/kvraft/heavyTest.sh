#!/bin/bash

set -e

for i in {1..20}
do
    tput reset
    echo $i
    LOG=1 go test -race -run $1 > $2 
done
