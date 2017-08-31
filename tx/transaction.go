package tx

const (
    LOGSIZE = 4*1024
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
    // currently do nothing
}

/* begin a transaction */
func Begin() {
    // currently do nothing
}

/* commit a transaction */
func Commit() {
    // currently do nothing
}

/* abort a transaction */
func Abort() {
    // currently do nothing
}

