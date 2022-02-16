#!/bin/sh

set -e

for i in {1..200}
do
		tput reset
    echo $i
    LOG=1 go test -run $1 -race > $2
done
