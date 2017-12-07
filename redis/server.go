package redis

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"pmem/region"
	"pmem/transaction"
	"strconv"
	_ "strings"
	_ "time"
)

type (
	server struct {
		db       *redisDb
		commands map[string](*redisCommand)
	}

	redisDb struct {
		dict   *dict
		expire *dict
	}

	redisCommand struct {
		name string
		proc func(*client)
		flag int
	}

	client struct {
		s       *server
		db      *redisDb
		conn    *net.TCPConn
		rBuffer *bufio.Reader
		wBuffer *bufio.Writer

		argc int
		argv [][]byte

		querybuf     []byte
		multibulklen int
		bulklen      int

		cmd *redisCommand
		tx  transaction.TX
	}

	sharedObjects struct {
		crlf, czero, cone, ok, nullbulk, emptybulk, syntaxerr, bulkhead, inthead, arrayhead []byte
	}
)

const (
	DATASIZE int    = 32000000
	UUID     int    = 9524
	PORT     string = ":6379"

	CMD_WRITE    int = 1
	CMD_READONLY int = 2
)

var (
	shared            sharedObjects
	redisCommandTable = [...]redisCommand{
		redisCommand{"GET", getCommand, CMD_READONLY},
		redisCommand{"GETRANGE", getrangeCommand, CMD_READONLY},
		redisCommand{"MGET", mgetCommand, CMD_READONLY},
		redisCommand{"EXISTS", existsCommand, CMD_READONLY},
		redisCommand{"DBSIZE", dbsizeCommand, CMD_READONLY},
		redisCommand{"RANDOMKEY", randomkeyCommand, CMD_READONLY},
		redisCommand{"STRLEN", strlenCommand, CMD_READONLY},
		redisCommand{"TTL", ttlCommand, CMD_READONLY},
		redisCommand{"PTTL", pttlCommand, CMD_READONLY},
		redisCommand{"APPEND", appendCommand, CMD_WRITE},
		redisCommand{"SET", setCommand, CMD_WRITE},
		redisCommand{"SETNX", setnxCommand, CMD_WRITE},
		redisCommand{"SETEX", setexCommand, CMD_WRITE},
		redisCommand{"PSETEX", psetexCommand, CMD_WRITE},
		redisCommand{"SETRANGE", setrangeCommand, CMD_WRITE},
		redisCommand{"GETSET", getsetCommand, CMD_WRITE},
		redisCommand{"MSET", msetCommand, CMD_WRITE},
		redisCommand{"MSETNX", msetnxCommand, CMD_WRITE},
		redisCommand{"DEL", delCommand, CMD_WRITE},
		redisCommand{"FLUSHDB", flushdbCommand, CMD_WRITE},
		redisCommand{"EXPIRE", expireCommand, CMD_WRITE},
		redisCommand{"EXPIREAT", expireatCommand, CMD_WRITE},
		redisCommand{"PEXPIRE", pexpireCommand, CMD_WRITE},
		redisCommand{"PEXPIREAT", pexpireatCommand, CMD_WRITE},
		redisCommand{"PERSIST", persistCommand, CMD_WRITE}}
)

func RunServer() {
	s := new(server)
	s.Start()
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

func (s *server) init(path string) {
	region.Init(path, DATASIZE, UUID)
	tx := transaction.NewUndo()
	s.db = &redisDb{NewDict(tx, 1024, 32), NewDict(tx, 128, 1)}
	transaction.Release(tx)
	s.populateCommandTable()
	createSharedObjects()
}

func (s *server) populateCommandTable() {
	s.commands = make(map[string](*redisCommand))
	for i, v := range redisCommandTable {
		s.commands[v.name] = &redisCommandTable[i]
	}
}

func createSharedObjects() {
	shared = sharedObjects{
		crlf:      []byte("\r\n"),
		czero:     []byte(":0\r\n"),
		cone:      []byte(":1\r\n"),
		ok:        []byte("+OK\r\n"),
		nullbulk:  []byte("$-1\r\n"),
		emptybulk: []byte("$0\r\n\r\n"),
		syntaxerr: []byte("-ERR syntax error\r\n"),
		bulkhead:  []byte("$"),
		inthead:   []byte(":"),
		arrayhead: []byte("*")}
}

func (s *server) Cron() {
	go s.db.Cron()
}

func (s *server) handleClient(conn *net.TCPConn) {
	// defer conn.Close()
	c := s.newClient(conn)
	c.conn.SetNoDelay(false) // try batching packet to improve tp.
	c.processInput()
	//go c.processOutput()
}

func (s *server) newClient(conn *net.TCPConn) *client {
	return &client{s: s,
		db:           s.db,
		conn:         conn,
		rBuffer:      bufio.NewReader(conn),
		wBuffer:      bufio.NewWriter(conn),
		argc:         0,
		argv:         nil,
		querybuf:     make([]byte, 1024),
		multibulklen: 0,
		bulklen:      -1,
		cmd:          nil,
		tx:           nil}
}

// Process input buffer and call command.
func (c *client) processInput() {
	pos := 0        // current buffer pos for network reading
	curr := 0       // curr pos of processing
	finish := false // finish processing a query
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
	if c.multibulklen == 0 {
		newline = findNewLine(c.querybuf[begin:end])
		if newline == -1 {
			return begin, false
		}
		if c.querybuf[begin] != '*' {
			fmt.Println("Protocal error! expected '*' for multibulk len ", begin, string(c.querybuf[begin:]))
			os.Exit(1)
		}
		// has to exclude '*'/'\r' with +1/-1
		size, err := slice2i(c.querybuf[begin+1 : begin+newline-1])
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
			size, err := slice2i(c.querybuf[begin+1 : begin+newline-1])
			fatalError(err)
			c.bulklen = size
			begin += newline + 1
		}

		// read bulk argument
		if end-begin < c.bulklen+2 {
			// not enough data (+2 == trailing \r\n)
			return begin, false
		} else {
			arg := make([]byte, c.bulklen)
			copy(arg, c.querybuf[begin:])
			c.argc += 1
			c.argv = append(c.argv, arg)
			begin += c.bulklen + 2
			c.bulklen = -1
			c.multibulklen--
		}
	}
	// successuflly process the whole mutlibulk query if reach here
	return begin, true
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

func (c *client) reset() {
	c.argc = 0
	c.argv = c.argv[0:0]
	c.multibulklen = 0
	c.bulklen = -1
}

// currenlty only support simple SET/GET that is used by memtierbenchmark
func (c *client) processCommand() {
	c.lookupCommand()
	if c.cmd == nil {
		c.notSupported()
	} else {
		if c.cmd.flag&CMD_READONLY > 0 {
			c.tx = transaction.NewReadonly()
		} else {
			c.tx = transaction.NewUndo()
		}
		c.tx.Begin()
		c.cmd.proc(c)
		c.tx.Commit()
		transaction.Release(c.tx)
		c.tx = nil
		c.wBuffer.Flush()
	}
}

func (c *client) lookupCommand() {
	c.cmd = c.s.commands[(string(c.argv[0]))]
}

func (c *client) notSupported() {
	fmt.Println("Command not supported!")
	for _, q := range c.argv {
		fmt.Print(string(q), " ")
	}
}

func (c *client) addReply(s []byte) {
	c.wBuffer.Write(s)
}

func (c *client) addReplyBulk(s []byte) {
	c.wBuffer.Write(shared.bulkhead)
	c.wBuffer.Write([]byte(strconv.Itoa(len(s)))) // not efficient
	c.wBuffer.Write(shared.crlf)
	c.wBuffer.Write(s)
	c.wBuffer.Write(shared.crlf)
}

func (c *client) addReplyLongLong(ll int) {
	c.wBuffer.Write(shared.inthead)
	c.wBuffer.Write([]byte(strconv.Itoa(ll))) // not efficient
	c.wBuffer.Write(shared.crlf)
}

func (c *client) addReplyMultiBulkLen(ll int) {
	c.wBuffer.Write(shared.arrayhead)
	c.wBuffer.Write([]byte(strconv.Itoa(ll))) // not efficient
	c.wBuffer.Write(shared.crlf)
}

func (c *client) addReplyError(err []byte) {
	c.wBuffer.Write([]byte("-ERR "))
	c.wBuffer.Write(err)
	c.wBuffer.Write(shared.crlf)
}

func (c *client) getLongLongFromObjectOrReply(o, msg []byte) (int64, error) {
	i, err := getLongLongFromObject(o)
	if err != nil {
		if msg != nil {
			c.addReplyError(msg)
		} else {
			c.addReplyError([]byte("value is not an integer or out of range"))
		}
	}
	return i, err
}

func getLongLongFromObject(o []byte) (int64, error) {
	return strconv.ParseInt(string(o), 10, 64)
}

func (c *client) cleanup() {
	c.conn.Close()
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
