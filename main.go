package main

import (
	"fmt"
	"math/rand"
	"os"
)

func main() {
	var path = "/home/aeternal/Documents/Projects/ScotiDB/test.txt";
	// Hello World as a byte array
	//var data = []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64}
	var data2 = []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F}
	SaveData2(path, data2)
}

func SaveData(path string, data []byte) error {
	// File Permissions 0644 -> -rw-rw-r--
	fp, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644);
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