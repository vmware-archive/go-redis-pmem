#/bin/bash

set -e

# Travis CI clones the repository at ~/vmware-samples/go-redis-pmem
# Move the repository into GOPATH first.
cd ..
mkdir -p $GOPATH/src/github.com/vmware-samples
mv go-redis-pmem $GOPATH/src/github.com/vmware-samples

# Build the modified Go compiler.
git clone --depth 1  https://github.com/jerrinsg/go-pmem ~/go-pmem
cd ~/go-pmem/src
./make.bash

# Force travis CI to use the compiler and toolchain we just built (TODO).
# If the go compiler is used as ~/go-pmem/bin/go test, travis uses the tools
# found in /home/travis/.gimme/versions/go1.11.1.linux.amd64 to do the build.

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
        GOROOT="$HOME/go-pmem/" GOTOOLDIR="$HOME/go-pmem/pkg/tool/linux_amd64" ~/go-pmem/bin/go test -tags="consistency" -v # run j
        sleep 1
    done
done
