package tests

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/vmware-samples/go-redis-pmem/redis"
)

// fileExists checks if a file exists and is not a directory
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func client1() {
	a, b := 0, 0
	var request string

	conn, err := net.Dial("tcp", "127.0.0.1:6379")
	errHandler(err)

	for i := 0; i < 20000; i++ {
		as := strconv.Itoa(a)
		bs := strconv.Itoa(b)
		alen := strconv.Itoa(len(as))
		blen := strconv.Itoa(len(bs))
		// Build a large request that client 1 will send to the redis server
		request += fmt.Sprintf("*3\r\n$3\r\nSET\r\n$1\r\nA\r\n$%s\r\n%s\r\n*3\r\n$3\r\nSET\r\n$1\r\nB\r\n$%s\r\n%s\r\n", alen, as, blen, bs)
		a++
		b++
	}

	// Run client 2 that will set the value of c
	go client2()

	fmt.Fprintf(conn, request)

	for {
	}
}

func client2() {
	conn, err := net.Dial("tcp", "127.0.0.1:6379")
	errHandler((err))

	// Sleep for some time so that client 1 gets a chance to run a few SET
	// iterations
	time.Sleep(time.Millisecond * 100)

	a, b, _ := getValues()
	c := strconv.Itoa(a + b)
	clen := strconv.Itoa(len(c))
	println("Client 2 - Got a as ", a, " and b as ", b, ". Set c as ", c)
	fmt.Fprintf(conn, "*3\r\n$3\r\nSET\r\n$1\r\nC\r\n$%s\r\n%s\r\n", clen, c)
	conn.Close()
	time.Sleep(time.Millisecond * 100)

	var iptr *int
	// induce a crash here
	println(*iptr)
}

func errHandler(err error) {
	if err != nil {
		log.Fatal((err))
	}
}

func getValues() (int, int, int) {
	conn, err := net.Dial("tcp", "127.0.0.1:6379")
	errHandler(err)

	fmt.Fprintf(conn, "*2\r\n$3\r\nGET\r\n$1\r\nA\r\n*2\r\n$3\r\nGET\r\n$1\r\nB\r\n*2\r\n$3\r\nGET\r\n$1\r\nC\r\n")
	br := bufio.NewReader(conn)
	var astr string
	var ac string

	var values [3]int

	for i := 0; i < 3; i++ {
		ac, err = br.ReadString('\n')
		errHandler((err))
		if ac != "$-1\r\n" {
			astr, err = br.ReadString('\n')
			errHandler((err))
		}
		if len(astr) > 0 {
			values[i], err = strconv.Atoi(astr[:len(astr)-2])
			errHandler((err))
		}
		astr = ""
	}
	conn.Close()
	return values[0], values[1], values[2]
}

func TestPipeline(t *testing.T) {
	// add this test to the tests folder but ignore it by default

	firstInit := !fileExists("./database")

	go redis.RunServer()

	// Sleep for 2 seconds to give redis server enough time to come up
	time.Sleep(time.Second * 2)

	if firstInit {
		client1()
	} else {
		a, b, c := getValues()
		println("Got values as: a = ", a, " b = ", b, " c = ", c)
		if c/2 > a || c/2 > b {
			t.Fatal("Invalid values")
		}
	}
}
