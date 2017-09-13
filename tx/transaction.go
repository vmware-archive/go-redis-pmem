package tx

const (
	LOGSIZE = 4 * 1024
)

/*
 * basic info of current transcation and
 * the inital log area.
 * currently just place holder
 */
type txHeader struct {
	log [LOGSIZE]byte
}

func Init(data []byte, size int) {
	/* currently only support simple undo logging. */
	initUndo(data[:LOGSIZE])
}

/* begin a transaction */
func Begin() {
	beginUndo()
}

/* commit a transaction */
func Commit() {
	commitUndo()
}

/* abort a transaction */
func Abort() {
	rollbackUndo()
}
