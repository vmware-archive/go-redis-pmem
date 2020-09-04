///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-3-Clause
///////////////////////////////////////////////////////////////////////

package main

import (
	"log"
	"os"

	"github.com/vmware-samples/go-redis-pmem/redis"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: ./app <dbName>")
	}
	redis.RunServer(os.Args[1])
}
