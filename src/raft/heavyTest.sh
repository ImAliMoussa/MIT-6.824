#!/bin/sh

set -e

for i in  {1..200}
do
echo $i
go test -run $1 -race > out
done