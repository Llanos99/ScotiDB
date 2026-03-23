package impl

type BTree struct {
	root uint64
	get  func(uint64) []byte
	new  func([]byte) uint64
	del  func(uint64)
}

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		panic(1)
	}
}

func (tree *BTree) Insert(key []byte, val []byte) {
	// no KV has been added before
	if tree.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.SetHeader(BNODE_NODE, 2)
		NodeAppendKV(root, 0, 0, nil, nil)
		NodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}
	// tree is not empty
	node := TreeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := NodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.SetHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.GetKey(0)
			// now root stores the pointers to the child nodes
			NodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		// BTree is now pointing to this new root
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

func (tree *BTree) Delete(key []byte) bool {
	return false
}
