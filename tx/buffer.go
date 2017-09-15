package tx

import (
	"errors"
	"unsafe"
)

type logBuffer interface {
	Write([]byte) (int, error)
	Read([]byte) (int, error)
	Tail() int
	Rewind(int)
	Clear()
}

/* A simple linear buffer */
type linearUndoBuffer struct {
	buffer   []byte
	tail     int
	capacity int
}

func initLinearUndoBuffer(buffer []byte, tail int) (logBuffer, error) {
	b := new(linearUndoBuffer)
	b.buffer = buffer
	b.tail = tail
	b.capacity = len(buffer)
	if b.capacity < b.tail {
		return nil, errors.New("tx.buffer: Fatal! Buffer tail out of range!")
	}
	return b, nil
}

func (b *linearUndoBuffer) Write(input []byte) (n int, err error) {
	remain := b.capacity - b.tail
	if len(input) > remain {
		return 0, errors.New("tx.buffer: Running out of log space!")
	}

	copy(b.buffer[b.tail:], input)
	Persist(unsafe.Pointer(&b.buffer[b.tail]), len(input))
	b.tail += len(input)
	return len(input), nil
}

/* undo buffer read BACKWARD to perform rollback in correct order. */
func (b *linearUndoBuffer) Read(output []byte) (n int, err error) {
	has := b.tail
	if len(output) > has {
		return 0, errors.New("tx.buffer: No enough log data for read!")
	}

	copy(output, b.buffer[b.tail-len(output):b.tail])
	Persist(unsafe.Pointer(&output[0]), len(output))
	b.tail -= len(output)
	return len(output), nil
}

func (b *linearUndoBuffer) Tail() int {
	return b.tail
}

func (b *linearUndoBuffer) Rewind(len int) {
	b.tail -= len
}

func (b *linearUndoBuffer) Clear() {
	b.tail = 0
}
