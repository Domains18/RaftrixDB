package transaction

import "github.com/Domains18/RaftrixDB.git/pkg/types"

// Op is a single operation inside a transaction.
type Op struct {
	Command types.Command
}

// Txn represents an atomic batch of operations that will be committed
// as a single Raft log entry.
type Txn struct {
	ops []Op
}

// NewTxn creates an empty transaction.
func NewTxn() *Txn {
	return &Txn{}
}

// Put enqueues a PUT operation.
func (t *Txn) Put(key string, value []byte) *Txn {
	t.ops = append(t.ops, Op{Command: types.Command{
		Op: types.OpPut, Key: key, Value: value,
	}})
	return t
}

// Delete enqueues a DELETE operation.
func (t *Txn) Delete(key string) *Txn {
	t.ops = append(t.ops, Op{Command: types.Command{
		Op: types.OpDelete, Key: key,
	}})
	return t
}

// CAS enqueues a compare-and-swap operation.
func (t *Txn) CAS(key string, prevValue, newValue []byte) *Txn {
	t.ops = append(t.ops, Op{Command: types.Command{
		Op: types.OpCAS, Key: key, Value: newValue, PrevValue: prevValue,
	}})
	return t
}

// Ops returns all operations in the transaction.
func (t *Txn) Ops() []Op { return t.ops }

// Len returns the number of operations.
func (t *Txn) Len() int { return len(t.ops) }