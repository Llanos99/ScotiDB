package main

import (
	"ScotiDB/impl"
	"fmt"
	"math/rand"
	"os"
)

func main() {
	//var path = "/home/aeternal/Documents/Projects/ScotiDB/test.txt"
	// Hello World as a byte array
	//var data = []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64}
	// var data2 = []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F}
	// SaveData2(path, data2)

	// Create an empty node with 4KB space
	node := impl.BNode(make([]byte, impl.BTREE_PAGE_SIZE))

	// Configure the header: LEAF NODE WITH 2 KEYS
	node.SetHeader(impl.BNODE_LEAF, 2)

	// Testing Key-Values pairs
	key1, val1 := []byte("apple"), []byte("red")
	key2, val2 := []byte("banana"), []byte("yellow")

	// Add offsets manually
	// KV1: klen(2B) + vlen(2B) + key(5B) + val(3B) = 12B
	node.SetOffset(1, 12)
	// KV2: offset_1 + klen(2B) + vlen(2B) + key(6B) + val(6B) = 12B + 16B = 28B
	node.SetOffset(2, 28)

	// Write real values on the node body
	node.WriteKV(0, key1, val1)
	node.WriteKV(1, key2, val2)

	// TEST
	fmt.Printf("Node type: %d\n", node.Btype())
	fmt.Printf("Number of keys: %d\n", node.Nkeys())
	fmt.Printf("Keys node total size: %d bytes\n", node.Nbytes())

	// Test getKey y getVal
	fmt.Printf("KV 0: %s -> %s\n", node.GetKey(0), node.GetVal(0))
	fmt.Printf("KV 1: %s -> %s\n", node.GetKey(1), node.GetVal(1))

	// Test binary search
	idx := impl.NodeLookupLE(node, []byte("apple"))
	fmt.Printf("Looking 'apple': index %d\n", idx)

	// Search for "azucar" (between apple and banana) -> should return 0
	idx = impl.NodeLookupLE(node, []byte("banana"))
	fmt.Printf("Looking 'azucar': index %d\n", idx)

	// Search "cherry" (greather than banana) -> should return 1
	idx = impl.NodeLookupLE(node, []byte("cherry"))
	fmt.Printf("Looking 'cherry': index %d\n", idx)
}

func SaveData(path string, data []byte) error {
	// File Permissions 0644 -> -rw-rw-r--
	fp, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}
	// After returning, close the file
	defer fp.Close()
	_, err = fp.Write(data)
	if err != nil {
		fmt.Println("Error: ", err)
		return err
	}
	return fp.Sync()
}

func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, rand.Intn(1000))
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_EXCL, 0664)
	if err != nil {
		fmt.Println("Error: ", err)
		return err
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()
	_, err = fp.Write(data)
	if err != nil {
		fmt.Println("Here 2")
		return err
	}
	err = fp.Sync()
	if err != nil {
		fmt.Println("Here 3")
		return err
	}
	fmt.Println("Here")
	return os.Rename(path, tmp)
}
