package fasterbs

/*
#include "/usr/include/faster/faster.h"
#cgo pkg-config: /usr/lib/pkgconfig/faster.pc --static
#cgo LDFLAGS: -L/usr/lib -lfaster -lm -luuid -ltbb -laio -lpthread -lgcc -lstdc++ -lstdc++fs
*/
import "C"
import (
	"context"
	"fmt"
	"log"
	"unsafe"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

// Blockstore is a sqlite backed IPLD blockstore, highly optimized and
// customized for IPLD query and write patterns.
type Blockstore struct {
	db *C.FasterDb
}

var _ blockstore.Blockstore = (*Blockstore)(nil)

type Options struct {
	// placeholder
}

const (
	OK = iota
	PENDING
	NOT_FOUND
	OUT_OF_MEMORY
	IO_ERROR
	CORRUPTION
	ABORTED
)

// Open creates a new faster-backed blockstore.
func Open(path string, _ Options) (*Blockstore, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// TODO: move to Options
	table_size := 1 << 15
	log_size := 1024 * 1024 * 1024
	log_mutable_fraction := 0.9
	pre_allocate_log := true
	db := C.f_open_db((C.ulong)(table_size), (C.ulong)(log_size), cPath, (C.double)(log_mutable_fraction), (C.bool)(pre_allocate_log))
	if db == nil {
		return nil, fmt.Errorf("failed to open faster database")
	}

	bs := &Blockstore{db: db}
	// C.f_start_session(bs.db)

	return bs, nil
}

func (b *Blockstore) Has(cid cid.Cid) (bool, error) {
	key, keylen := keyFromCid(cid)
	defer C.free(key)

	cHas := C.f_has(b.db, (*C.char)(key), (C.ulong)(keylen), 1)
	if cHas == 1 {
		return true, nil
	}

	return false, nil
}

func (b *Blockstore) Get(cid cid.Cid) (blocks.Block, error) {
	key, keylen := keyFromCid(cid)
	defer C.free(key)
	val := (*C.char)(C.malloc(0))
	vallen := C.size_t(0)

	ret := C.f_get(b.db, (*C.char)(key), (C.ulong)(keylen), &val, &vallen, 1)
	defer C.f_free_buf(val, vallen)

	if ret == NOT_FOUND {
		return nil, blockstore.ErrNotFound
	}
	
	return blocks.NewBlockWithCid(C.GoBytes(unsafe.Pointer(val), (C.int)(vallen)), cid)
}

func (b *Blockstore) GetSize(cid cid.Cid) (int, error) {
	key, keylen := keyFromCid(cid)
	defer C.free(key)

	size := C.f_get_len(b.db, (*C.char)(key), (C.ulong)(keylen), 1)
	if size == -1 {
		return -1, blockstore.ErrNotFound
	}
	return (int)(size), nil
}

func (b *Blockstore) Put(block blocks.Block) error {
	err := b.put(block)
	// C.f_complete_pending(b.db, false)
	// C.f_checkpoint(b.db)

	return err
}

func (b *Blockstore) put(block blocks.Block) error {
	var (
		cid  = block.Cid()
		data = block.RawData()
	)

	key, keylen := keyFromCid(cid)
	defer C.free(key)
	val, vallen := C.CBytes(data), len(data)
	defer C.free(val)

	ret := C.f_set(b.db, (*C.uchar)(key), (C.ulong)(keylen), (*C.uchar)(val), (C.ulong)(vallen), 1)
	if ret != OK {
		return fmt.Errorf("failed to put block")
	}

	return nil
}

func (b *Blockstore) PutMany(blocks []blocks.Block) error {
	var err error
	for _, blk := range blocks {
		if err = b.put(blk); err != nil {
			break
		}
	}
	
	return err
}

func (b *Blockstore) DeleteBlock(cid cid.Cid) error {
	key, keylen := keyFromCid(cid)
	defer C.free(key)

	ret := C.f_del(b.db, (*C.char)(key), (C.ulong)(keylen), 1)
	if ret != OK {
		return fmt.Errorf("failed to delete block")
	}
	// C.f_complete_pending(b.db, false)
	// C.f_checkpoint(b.db)
	return nil
}

func (b *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ret := make(chan cid.Cid)

	cIter := C.f_iter(b.db)
	if cIter == nil {
		close(ret)
		return nil, fmt.Errorf("failed to query all keys from sqlite3 blockstore")
	}

	go func() {
		var key *C.char
		key = (*C.char)(C.malloc(0))
		keylen := C.size_t(0)

		defer func() {
			close(ret)
			C.f_free_iter(cIter)
			C.f_free_buf(key, keylen)
		}()

		for C.f_iter_next_key(cIter, &key, (*C.ulong)(&keylen)) != 0 {
			goBytes := C.GoBytes(unsafe.Pointer(key), (C.int)(keylen))
			id, err := cid.Cast(goBytes)
			if err != nil {
				log.Printf("failed to parse multihash when querying all keys in sqlite3 blockstore: %s: %v %d", err, goBytes, keylen)
			}
			ret <- id
		}
	}()
	return ret, nil
}

func (b *Blockstore) HashOnRead(_ bool) {
	log.Print("faster blockstore ignored HashOnRead request")
}

func (b *Blockstore) Close() error {
	if b.db != nil {
		C.f_complete_pending(b.db, true)
		// C.f_stop_session(b.db)
		C.f_close(b.db)
		b.db = nil
	}
	return nil
}

func keyFromCid(c cid.Cid) (unsafe.Pointer, int) {
	bytes := c.Bytes()
	return C.CBytes(bytes), len(bytes)
}
