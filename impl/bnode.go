package impl

import (
	"bytes"
	"encoding/binary"
)

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

type BNode []byte

func (node BNode) Btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) Nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) SetHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// pointers
func (node BNode) GetPtr(idx uint16) uint64 {
	if idx >= node.Nkeys() {
		panic("GetPtr: index out of bounds")
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) SetPtr(idx uint16, val uint64) {
	if idx >= node.Nkeys() {
		panic("SetPtr: index out of bounds")
	}
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:pos+8], val)
}

// offset list
func OffsetPos(node BNode, idx uint16) uint16 {
	if idx < 1 || idx > node.Nkeys() {
		panic("OffsetPos: index out of bounds")
	}
	return HEADER + 8*node.Nkeys() + 2*(idx-1)
}

func (node BNode) GetOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[OffsetPos(node, idx):])
}

func (node BNode) SetOffset(idx uint16, offset uint16) {
	if idx < 1 || idx > node.Nkeys() {
		panic("SetOffset: index out of bounds")
	}
	pos := OffsetPos(node, idx)
	binary.LittleEndian.PutUint16(node[pos:pos+2], offset)
}

// key-values
func (node BNode) KvPos(idx uint16) uint16 {
	if idx > node.Nkeys() {
		panic("KvPos: index out of bounds")
	}
	return HEADER + 8*node.Nkeys() + 2*node.Nkeys() + node.GetOffset(idx)
}

func (node BNode) GetKey(idx uint16) []byte {
	if idx >= node.Nkeys() {
		panic("GetKey: index out of bounds")
	}
	pos := node.KvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) GetVal(idx uint16) []byte {
	if idx >= node.Nkeys() {
		panic("GetVal: index out of bounds")
	}
	pos := node.KvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos : pos+2])
	vlen := binary.LittleEndian.Uint16(node[pos+2 : pos+4])
	return node[pos+4:][klen : klen+vlen]
}

// node size in bytes
func (node BNode) Nbytes() uint16 {
	return node.KvPos(node.Nkeys())
}

// seek functions
func NodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.Nkeys()
	if nkeys <= 1 {
		return 0
	}
	found := uint16(0)
	low, high := uint16(1), nkeys-1
	for low <= high {
		mid := low + (high-low)/2
		cmp := bytes.Compare(node.GetKey(mid), key)
		if cmp <= 0 {
			found = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return found
}

// aux function to write the bytes of a KV in the node
func (node BNode) WriteKV(idx uint16, key []byte, val []byte) {
	pos := node.KvPos(idx)
	lKey := uint16(len(key))
	lVal := uint16(len(val))
	binary.LittleEndian.PutUint16(node[pos:], lKey)
	binary.LittleEndian.PutUint16(node[pos+2:pos+4], lVal)
	copy(node[pos+4:pos+4+lKey], key)
	valPos := pos + 4 + lKey
	copy(node[valPos:valPos+lVal], val)
}
