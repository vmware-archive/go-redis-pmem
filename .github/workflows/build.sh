#!/bin/bash

set -e

# Build the modified Go compiler.
git clone --depth 1  https://github.com/jerrinsg/go-pmem ~/go-pmem
cd ~/go-pmem/src
./make.bash

# Get go-pmem-transaction repository
GOROOT="$HOME/go-pmem/" GOTOOLDIR="$HOME/go-pmem/pkg/tool/linux_amd64" ~/go-pmem/bin/go get github.com/vmware/go-pmem-transaction/...

cd $GOPATH/src/github.com/vmware-samples/go-redis-pmem/tests

# 1. Run the consistency check 10 times in a loop
DATAFILE="$GOPATH/src/github.com/vmware-samples/go-redis-pmem/tests/database"
for i in {1..10}
do
    if [ -e $DATAFILE ]
    then
        rm $DATAFILE
    fi
    for j in {1..2}
    do
        GOROOT="$HOME/go-pmem/" GOTOOLDIR="$HOME/go-pmem/pkg/tool/linux_amd64" ~/go-pmem/bin/go test -txn -tags="consistency" -v # run j
        sleep 1
    done
done