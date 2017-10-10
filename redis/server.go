package redis

import (
	"os"
	"fmt"
	"net"
	"strconv"
	"pmem/region"
	"pmem/transaction"
)

const (
	DATASIZE int 	= 10000000
	UUID 	 int 	= 9524
	PORT	 string = ":6379"
)

type (
	server struct {
		db    		*dict
		token	 	chan bool
	}

	client struct {
		s 				*server
		db       		*dict
		conn     		net.Conn
		argc	 		int
		argv	 		[][]byte
		querybuf 		[]byte
		reply	 		[]byte
		multibulklen 	int
		bulklen	 		int
		reppos			int
	}
)

func (s *server) init(path string) {
	region.Init(path, DATASIZE, UUID)
	tx := transaction.NewUndo()
	s.db = NewDict(tx)
	transaction.Release(tx)
	s.token = make(chan bool, 1) // used as mutex to protect db
	s.token <- true
}

func (s *server) Start() {
	// init database
	s.init("test_server")

	// accept client connections
	tcpAddr, err := net.ResolveTCPAddr("tcp4", PORT)
	fatalError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	fatalError(err)

	for {
        conn, err := listener.Accept()
        if err != nil {
            continue
        }
        // run as a goroutine
        go s.handleClient(conn)
    }
}

func (s *server) handleClient(conn net.Conn) {
	defer conn.Close()
	c := s.newClient(conn)
	c.processInput()
}

func (s * server) newClient(conn net.Conn) *client {
	return &client{s, s.db, conn, 0, nil, make([]byte, 1024), make([]byte, 1024), 0, -1, 0}
}

// Process input buffer and call command.
func (c *client) processInput() {
	pos := 0   // current buffer pos for network reading
	curr := 0  // curr pos of processing
	finish := false   // finish processing a query
	for {
		n, err := c.conn.Read(c.querybuf[pos:])
		if err != nil && curr >= pos {
			return
		}
		//fmt.Println("Input buffer ", pos, n, len(c.querybuf), curr)
		//fmt.Printf("%q\n", string(c.querybuf))

		// process multi bulk buffer
		curr, finish = c.processMultibulkBuffer(curr, pos + n)
		if finish {
			// Get one full query
			if c.argc > 0 {
				//c.printArgs()
				<- c.s.token
				c.processCommand()
				c.s.token <- true
			}
			c.reset()
		}
		
		pos += n
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

func (c *client) printArgs() {
	fmt.Println("Client has", c.argc, "arguments:")
	for _,q := range c.argv {
		fmt.Println(string(q))
	}
}

func (c *client) reset() {
	c.argc = 0
	c.argv = nil
	c.multibulklen = 0
	c.bulklen = -1
}

func (c *client) processCommand() {
	if string(c.argv[0]) == "SET" && c.argc == 3 {
		tx := transaction.NewUndo()
		//fmt.Println("Set", string(c.argv[1]), string(c.argv[2]))
		c.db.Set(tx, string(c.argv[1]), string(c.argv[2]))
		transaction.Release(tx)
		c.addReply("+OK\r\n")
	} else if string(c.argv[0]) == "GET" && c.argc == 2 {
		tx := transaction.NewUndo()
		v := c.db.Get(tx, string(c.argv[1]))
		//fmt.Println("Get", string(c.argv[1]), v)
		transaction.Release(tx)
		if v == "" {
			c.addReply("$-1\r\n")
		} else {
			c.addReplyBulk(v)
		}
	} else {
		fmt.Println("Command not supported!")
		c.printArgs()
	}
}

func (c *client) addReply(s string) {
	copy(c.reply[c.reppos:], s)
	c.reppos += len(s)
	//fmt.Println("Reply buffer ", c.reppos)
	//fmt.Printf("%q\n", string(c.reply))
	n, err := c.conn.Write(c.reply[:c.reppos])
	fatalError(err)
	if n > 0 {
		copy(c.reply, c.reply[n:c.reppos])
		c.reppos -= n
	}
}

func (c *client) addReplyBulk(s string) {
	c.addReply("$" + strconv.Itoa(len(s))+"\r\n")
	c.addReply(s+"\r\n")
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
