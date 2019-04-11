# go-redis-pmem

## Overview
This is a Go implementation of Redis designed for persistent memory. Only a
limited subset of Redis commands are currently supported.

## Try it out

### Prerequisites

To use these packages, you need new extensions to the Go language. These changes
are maintained in a separate repository [here](https://github.com/jerrinsg/go-pmem).

### Build & Run

1. Download and build Go compiler designed for persistent memory. Follow
instructions [here](https://github.com/jerrinsg/go-pmem).

2. ```go get -v -u github.com/vmware-samples/go-redis-pmem/...```
Make sure to use the Go binary built in step 1.

3. To build and run the example application
```
$ cd example
$ go build app.go
$ ./app
```
Make sure to use the Go binary built in step 1.

## Documentation

This is a Go version of Redis designed for persistent memory. It uses the
functionalities provided by the [go-pmem-transaction](https://github.com/vmware/go-pmem-transaction)
library in its implementation.
This implementation of Redis only supports a limited set of Redis commands.
Please refer to `redis/server.go` for the list of supported commands.
It is put out as an implementation example of the [go-pmem-transaction](https://github.com/vmware/go-pmem-transaction)
library.

## Contributing

The go-redis-pmem project team welcomes contributions from the community. Before you start working with go-redis-pmem, please
read our [Developer Certificate of Origin](https://cla.vmware.com/dco). All contributions to this repository must be
signed as described on that page. Your signature certifies that you wrote the patch or have the right to pass it on
as an open-source patch. For more detailed information, refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## License
go-redis-pmem is available under BSD-2 license.
