package impl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
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

// insert keys into leafs
// 1. Get the position to insert using NodeLookupLE
// 2. Copy the keys into a new node and insert the new key (cope-on-write strategy)
func LeafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.SetHeader(BNODE_LEAF, old.Nkeys()+1)
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendKV(new, idx, 0, key, val)
	NodeAppendRange(new, old, idx+1, idx, old.Nkeys()-idx)
}

// leafUpdate is similar to leafInsert; it updates an existing key instead of inserting a duplicate key.
func LeafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.SetHeader(BNODE_LEAF, old.Nkeys())
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendKV(new, idx, 0, key, val)
	NodeAppendRange(new, old, idx+1, idx+1, old.Nkeys()-(idx+1))
}

// remove a key from a LEAF node
func LeafDelete(new BNode, old BNode, idx uint16) {
	new.SetHeader(BNODE_LEAF, old.Nkeys()-1)
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendRange(new, old, idx, idx+1, old.Nkeys()-(idx+1))
}

// insert into internal nodes
func NodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.SetHeader(BNODE_NODE, old.Nkeys()+inc-1)
	NodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		NodeAppendKV(new, idx+uint16(i), tree.new(node), node.GetKey(0), nil)

	}
	NodeAppendRange(new, old, idx+inc, idx+1, old.Nkeys()-(idx+1))
}

// replace two adjacent links with 1
func NodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.SetHeader(BNODE_NODE, old.Nkeys()-1)
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendKV(new, idx, ptr, key, nil)
	NodeAppendRange(new, old, idx+1, idx+2, old.Nkeys()-(idx+2))
}

func NodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	new.SetPtr(idx, ptr)
	pos := new.KvPos(idx)
	lKey := uint16(len(key))
	lVal := uint16(len(val))
	binary.LittleEndian.PutUint16(new[pos:], lKey)
	binary.LittleEndian.PutUint16(new[pos+2:], lVal)
	copy(new[pos+4:], key)
	copy(new[pos+4+lKey:], val)
	new.SetOffset(idx+1, new.GetOffset(idx)+4+uint16((len(key)+len(val))))
}

// copy multiple KVs into the position from the old node
func NodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	if n == 0 {
		return
	}
	for i := uint16(0); i < n; i++ {
		new.SetPtr(dstNew+i, old.GetPtr(srcOld+i))
	}
	beginOld := old.KvPos(srcOld)
	endOld := old.KvPos(srcOld + n)
	copy(new[new.KvPos(dstNew):], old[beginOld:endOld])
	baseOffset := new.GetOffset(dstNew)
	for i := uint16(1); i <= n; i++ {
		oldRelativeOffset := old.GetOffset(srcOld+i) - old.GetOffset(srcOld)
		new.SetOffset(dstNew+i, baseOffset+oldRelativeOffset)
	}
}

// split node into two new nodes ensuring 2nd node always fit in page (4096)
func NodeSplit2(left BNode, right BNode, old BNode) {
	nkeys := old.Nkeys()
	splitIdx := nkeys
	for i := uint16(1); i <= nkeys; i++ {
		newSplitIdx := nkeys - i
		size := _calculateSize(old, newSplitIdx)
		if size > BTREE_PAGE_SIZE {
			break
		}
		splitIdx = newSplitIdx
	}
	if splitIdx == nkeys {
		splitIdx = nkeys - 1
	}
	left.SetHeader(old.Btype(), splitIdx)
	NodeAppendRange(left, old, 0, 0, splitIdx)
	right.SetHeader(old.Btype(), nkeys-splitIdx)
	NodeAppendRange(right, old, 0, splitIdx, nkeys-splitIdx)
}

// split node if it's too big. It will be splited into 1~3 nodes
func NodeSplit3(old BNode) (uint16, [3]BNode) {
	// no split needed
	if old.Nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	NodeSplit2(left, right, old)
	if left.Nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	newLeft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	NodeSplit2(newLeft, middle, left)
	if newLeft.Nbytes() > BTREE_PAGE_SIZE {
		panic(fmt.Sprintf(
			"NodeSplit3: critical invariant violated. Remaining node size (%d bytes) "+
				"exceeds page limit (%d bytes) after 2 splits. Check BTREE_MAX_KEY/VAL_SIZE limits.",
			newLeft.Nbytes(), BTREE_PAGE_SIZE,
		))
	}
	return 3, [3]BNode{newLeft, middle, right}
}

func ShouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	// avoid thrashing: 25% threshold
	if updated.Nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	// left priority: return -1
	if idx > 0 {
		sibling := BNode(tree.get(node.GetPtr(idx - 1)))
		merged := sibling.Nbytes() + node.Nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}
	// right node choice: return +1
	if idx+1 < node.Nkeys() {
		sibling := BNode(tree.get(node.GetPtr(idx + 1)))
		merged := sibling.Nbytes() + node.Nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling // left
		}
	}
	// no sibling was selected
	return 0, BNode{}
}

// merge two nodes into one
func NodeMerge(new BNode, left BNode, right BNode) {
	if left.Nbytes()+right.Nbytes()-HEADER > BTREE_PAGE_SIZE {
		panic("NodeMerge: Bytes sum exceed BTREE_PAGE_SIZE")
	}
	if left.Btype() != right.Btype() {
		panic("NodeMerge: Different node types")
	}
	new.SetHeader(left.Btype(), left.Nkeys()+right.Nkeys())
	// merge the pointer section: how to keep the ordering?
	// not needed, we are spliting nodes already ordered. KVs from left node are always <= KVs from right node
	// merge left node first
	NodeAppendRange(new, left, 0, 0, left.Nkeys()) // NodeAppend already do the dirty work: handle ptr + offset section
	// merge right node then
	NodeAppendRange(new, right, left.Nkeys(), 0, right.Nkeys())
}

func TreeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // Initialize the BNode
	idx := NodeLookupLE(node, key)                // Where to insert the key?
	switch node.Btype() {
	case BNODE_NODE:
		NodeInsert(tree, new, node, idx, key, val)
	case BNODE_LEAF:
		// key found, update it
		if bytes.Equal(key, node.GetKey(idx)) {
			LeafUpdate(new, node, idx, key, val)
		} else {
			// key wasn't found, add it to the right of the found index
			LeafInsert(new, node, idx+1, key, val)
		}
	default:
		panic("Bad node, can't insert it to the B+Tree")
	}
	return new
}

// delete a key from the tree
func TreeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := NodeLookupLE(node, key)
	switch node.Btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.GetKey(idx)) {
			return BNode{}
		}
		new := BNode(make([]byte, BTREE_PAGE_SIZE))
		LeafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return NodeDelete(tree, node, idx, key)
	default:
		panic("TreeDelete: Bad node type")
	}
}

func NodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	keyPointer := node.GetPtr(idx)
	keyNode := TreeInsert(tree, tree.get(keyPointer), key, val) // recursive call
	nsplit, split := NodeSplit3(keyNode)
	// deallocate the split node
	tree.del(keyPointer)
	// update keys
	NodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
}

func NodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.GetPtr(idx)
	updated := TreeDelete(tree, tree.get(kptr), key)
	// not found
	if len(updated) == 0 {
		return BNode{}
	}
	tree.del(kptr)
	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	// check if any merging is needed
	mergeDir, sibling := ShouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir == -1: // LEFT
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		NodeMerge(merged, sibling, updated)
		tree.del(node.GetPtr(idx - 1))
		NodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.GetKey(0))
	case mergeDir == +1: // RIGHT
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		NodeMerge(merged, updated, sibling)
		tree.del(node.GetPtr(idx + 1))
		NodeReplace2Kid(new, node, idx, tree.new(merged), merged.GetKey(0))
	case mergeDir == 0 && updated.Nkeys() == 0:
		if node.Nkeys() != 1 || idx != 0 {
			panic("NodeDelete: 1 empty child but no siblings violated")
		}
		new.SetHeader(BNODE_NODE, 0) // parents become empty too
	case mergeDir == 0 && updated.Nkeys() > 0: // no merge
		NodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// node printer
func (node BNode) DumpNode() {
	nkeys := node.Nkeys()
	btype := node.Btype()

	typeName := "INTERNAL"
	if btype == BNODE_LEAF {
		typeName = "LEAF"
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf(" NODE DUMP (%s) | Keys: %d | Total Size: %d bytes\n", typeName, nkeys, node.Nbytes())
	fmt.Println(strings.Repeat("=", 60))

	// 1. HEADER (Bytes 0-4)
	fmt.Printf("[00-04] HEADER:  Type=%d, NKeys=%d\n", btype, nkeys)

	// 2. POINTERS (8 bytes cada uno, empiezan en byte 4)
	ptrStart := uint16(4)
	fmt.Printf("[%02d-%02d] PTRS:   ", ptrStart, ptrStart+(nkeys*8))
	for i := uint16(0); i < nkeys; i++ {
		fmt.Printf("[%d: %d] ", i, node.GetPtr(i))
	}
	fmt.Println()

	// 3. OFFSETS (2 bytes cada uno, después de los punteros)
	offStart := 4 + (nkeys * 8)
	fmt.Printf("[%02d-%02d] OFFSETS: ", offStart, offStart+(nkeys*2))
	for i := uint16(1); i <= nkeys; i++ {
		fmt.Printf("[%d: %d] ", i, node.GetOffset(i))
	}
	fmt.Println()

	// 4. DATA (KV Pairs)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("%-10s | %-8s | %-8s | %-20s\n", "POSITION", "KLEN/VLEN", "INDEX", "CONTENT (Key:Val)")
	fmt.Println(strings.Repeat("-", 60))

	for i := uint16(0); i < nkeys; i++ {
		pos := node.KvPos(i)

		// Leemos klen/vlen directamente del slice para verificar integridad
		klen := binary.LittleEndian.Uint16(node[pos : pos+2])
		vlen := binary.LittleEndian.Uint16(node[pos+2 : pos+4])

		key := string(node.GetKey(i))
		val := string(node.GetVal(i))

		fmt.Printf("Byte %-5d | %d / %-5d | Idx %-3d | %s : %s\n",
			pos, klen, vlen, i, key, val)
	}
	fmt.Println(strings.Repeat("=", 60))
}

func _calculateSize(old BNode, splitIdx uint16) uint16 {
	nKeysOld := old.Nkeys()
	nKeysNew := nKeysOld - splitIdx
	size := uint16(4) + (nKeysNew)*8 + (nKeysNew)*2 + (old.GetOffset(nKeysOld) - old.GetOffset(splitIdx))
	return size
}
