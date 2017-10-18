package redis

import (
	"os"
	"fmt"
	"net"
	"time"
	"bufio"
	"strconv"
	"pmem/region"
	"pmem/transaction"
)

const (
	DATASIZE int 	= 500000000
	UUID 	 int 	= 9524
	PORT	 string = ":6379"
)

var (
	crlf		[]byte = []byte("\r\n")
	ok          []byte = []byte("+OK\r\n")
	nullbulk	[]byte = []byte("$-1\r\n")
	bulkhead	[]byte = []byte("$")

	foo			[]byte = []byte("foo")
)

type (
	server struct {
		db    		*dict
	}

	client struct {
		s 				*server
		db       		*dict
		conn     		*net.TCPConn
		rBuffer			*bufio.Reader
		wBuffer 		*bufio.Writer
		argc	 		int
		argv	 		[][]byte
		querybuf 		[]byte
		multibulklen 	int
		bulklen	 		int
	}
)

func (s *server) init(path string) {
	region.Init(path, DATASIZE, UUID)
	tx := transaction.NewUndo()
	s.db = NewDict(tx)
	transaction.Release(tx)
}

func (s *server) Start() {
	// init database
	s.init("test_server")

	// accept client connections
	tcpAddr, err := net.ResolveTCPAddr("tcp4", PORT)
	fatalError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	fatalError(err)

	go s.Cron()

	for {
        conn, err := listener.AcceptTCP()
        if err != nil {
            continue
        }
        // run as a goroutine
        go s.handleClient(conn)
    }
}

func (s *server) Cron() {
	for {
		time.Sleep(10 * time.Millisecond)
		undoTx := transaction.NewUndo()
		s.db.ResizeIfNeeded(undoTx)
		s.db.Rehash(undoTx, 10)
		transaction.Release(undoTx)
	}
}

func (s *server) handleClient(conn *net.TCPConn) {
	// defer conn.Close()
	c := s.newClient(conn)
	c.conn.SetNoDelay(false) // try batching packet to improve tp.
	go c.processInput()
	//go c.processOutput()
}

func (s * server) newClient(conn *net.TCPConn) *client {
	return &client{s, s.db, conn, bufio.NewReader(conn), bufio.NewWriter(conn), 0, nil, make([]byte, 1024), 0, -1}
}

// Process input buffer and call command.
func (c *client) processInput() {
	pos := 0   // current buffer pos for network reading
	curr := 0  // curr pos of processing
	finish := false   // finish processing a query
	for {
		n, err := c.conn.Read(c.querybuf[pos:])
		if err != nil {
			return
		}

		pos += n
		//fmt.Println("Input buffer ", pos, n, len(c.querybuf), curr)
		//fmt.Printf("%q\n", string(c.querybuf))

		// process multi bulk buffer
		curr, finish = c.processMultibulkBuffer(curr, pos)
		for finish {
			// Get one full query
			if c.argc > 0 {
				c.processCommand()
			}
			c.reset()
			curr, finish = c.processMultibulkBuffer(curr, pos)
		}
		
		// rewind query buffer if full.
		if pos == len(c.querybuf) {
			copy(c.querybuf, c.querybuf[curr:])
			pos -= curr
			curr = 0
		}
	}
}

// Process RESP array of bulk strings, e.g., "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"
func (c *client) processMultibulkBuffer(begin, end int) (int, bool) {
	newline := -1
	if (c.multibulklen == 0) {
		newline = findNewLine(c.querybuf[begin:end])
		if newline == -1 {
			return begin, false
		}
		if c.querybuf[begin] != '*' {
			fmt.Println("Protocal error! expected '*' for multibulk len ", begin, string(c.querybuf[begin:]))
			os.Exit(1)
		}
		// has to exclude '*'/'\r' with +1/-1
		size, err := slice2i(c.querybuf[begin+1:begin+newline-1])
		fatalError(err)
		begin += newline + 1
		if size <= 0 {
			return begin, true
		}
		c.multibulklen = size
	}
	for c.multibulklen > 0 {
		// read bulk len if unknown
		if c.bulklen == -1 {
			newline = findNewLine(c.querybuf[begin:end])
			if newline == -1 {
				return begin, false
			}
			if c.querybuf[begin] != '$' {
				fmt.Println("Protocal error! expected '$' for bulk len ", begin, string(c.querybuf[begin:]))
				os.Exit(1)
			}
			// has to exclude '$'/'\r' with +1/-1
			size, err := slice2i(c.querybuf[begin+1:begin+newline-1])
			fatalError(err)
			c.bulklen = size
			begin += newline + 1
		}

		// read bulk argument
		if end - begin < c.bulklen + 2 {
			// not enough data (+2 == trailing \r\n)
			return begin, false
		} else {
			arg := make([]byte, c.bulklen)
			copy(arg, c.querybuf[begin:])
			c.argc += 1
			c.argv = append(c.argv, arg)
			begin += c.bulklen + 2
			c.bulklen = -1;
            c.multibulklen--;
		}
	}
	// successuflly process the whole mutlibulk query if reach here 
	return begin, true
}

func printArgs(argv [][]byte) {
	fmt.Println("Client has", len(argv), "arguments:")
	for _,q := range argv {
		fmt.Println(string(q))
	}
}

func (c *client) reset() {
	c.argc = 0
	c.argv = c.argv[0:0]
	c.multibulklen = 0
	c.bulklen = -1
}

// currenlty only support simple SET/GET that is used by memtierbenchmark
func (c *client) processCommand() {
	if string(c.argv[0]) == "SET" && len(c.argv) == 3 {
		tx := transaction.NewUndo()
		//fmt.Println("Set", string(c.argv[1]), string(c.argv[2]))
		c.db.Set(tx, c.argv[1], c.argv[2])
		transaction.Release(tx)
		c.addReply(ok)
	} else if string(c.argv[0]) == "GET" && len(c.argv) == 2 {
		tx := transaction.NewReadonly()
		v := c.db.Get(tx, c.argv[1])
		//fmt.Println("Get", string(c.argv[1]), v)
		transaction.Release(tx)
		if len(v) == 0 {
			c.addReply(nullbulk)
			//c.addReplyBulk(foo)
		} else {
			c.addReplyBulk(v)
		}
	} else {
		fmt.Println("Command not supported!")
		printArgs(c.argv)
	}
}

func (c *client) addReply(s []byte) {
	c.wBuffer.Write(s)
	c.wBuffer.Flush()
}

func (c *client) addReplyBulk(s []byte) {
	c.wBuffer.Write(bulkhead)
	c.wBuffer.Write([]byte(strconv.Itoa(len(s))))
	c.wBuffer.Write(crlf)
	c.wBuffer.Write(s)
	c.wBuffer.Write(crlf)
	c.wBuffer.Flush()
}

func (c *client) cleanup() {
	c.conn.Close()
}

// Return index of newline in slice, -1 if not found
func findNewLine(buf []byte) int {
	for i, c := range buf[:] {
		if c == '\n' {
			return i
		}
	}
	return -1
}

func slice2i(buf []byte) (int, error) {
	return strconv.Atoi(string(buf))
}

func fatalError(err error) {
    if err != nil {
        fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
        os.Exit(1)
    }
}

func getClient() net.Conn {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", PORT)
	fatalError(err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	fatalError(err)

	return conn
}
