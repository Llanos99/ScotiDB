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
	oldNode := impl.BNode(make([]byte, impl.BTREE_PAGE_SIZE))
	oldNode.SetHeader(impl.BNODE_LEAF, 4)

	k1, v1 := []byte("berry"), []byte("blue")       // 4 + 5 + 4 = 13 bytes
	k2, v2 := []byte("dragonfruit"), []byte("pink") // 4 + 11 + 4 = 19 bytes
	k3, v3 := []byte("fig"), []byte("purple")       // 4 + 3 + 6 = 13 bytes
	k4, v4 := []byte("grape"), []byte("green")      // 4 + 5 + 5 = 14 bytes

	// OFFSET MANUAL SETUP
	// Offset 0 is always 0
	oldNode.SetOffset(1, 13)
	oldNode.SetOffset(2, 13+19)
	oldNode.SetOffset(3, 32+13)
	oldNode.SetOffset(4, 45+14)

	// MANUAL WRITE OF BYTES
	oldNode.WriteKV(0, k1, v1)
	oldNode.WriteKV(1, k2, v2)
	oldNode.WriteKV(2, k3, v3)
	oldNode.WriteKV(3, k4, v4)

	fmt.Println("ORIGINAL NODE (MANUAL SETUP)")
	oldNode.DumpNode()

	// Insert "cherry" : "red" at position 1
	newKey, newVal := []byte("cherry"), []byte("red") // 4 + 6 + 3 = 13 bytes
	targetIdx := uint16(1)

	newNode := impl.BNode(make([]byte, impl.BTREE_PAGE_SIZE))
	impl.LeafInsert(newNode, oldNode, targetIdx, newKey, newVal)

	fmt.Println("\nAFTER INSERTION")
	newNode.DumpNode()
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
