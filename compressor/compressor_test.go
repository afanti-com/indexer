package compressor

import (
	"testing"
	"fmt"
	"os"
	"bytes"
	"reflect"
)


func TestEncodeAndDecode(t *testing.T) {

	raw := []uint32{0, 32, 2003, 60006, 300009, 16777218}
	
	fmt.Println("before encode:", raw)
	
	var b bytes.Buffer
	var total_len uint = 0
	for _, x := range raw {
		len := Encode(&b, x)
		total_len += len
	}

	fmt.Println("total encode len: ", total_len)
	fmt.Println("encode result:", b.Bytes())
	
	// fmt.Println(len(b.Bytes()))
	// fmt.Println(len(b.String()))

	fout, err := os.OpenFile("compress.dat", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Errorf("open file error")
	}
	defer fout.Close()

	n, err := b.WriteTo(fout)
	if err != nil {
		t.Errorf("writeto file error")
	}

	if total_len != uint(n) {
		t.Errorf("write %d bytes not equals total_len %d", n, total_len)
	}
	
	fmt.Println("writeto file bytes: ", n)

	fin, err := os.Open("compress.dat")
	if err != nil {
		t.Errorf("open file error")
	}

	var buf bytes.Buffer
	bb := make([]byte, total_len)
	m, err := fin.Read(bb)
	if err != nil {
		t.Errorf("readfrom file error")
	}
	if m != int(n) {
		t.Errorf("read %d bytes from file not equals %d", m, n)
	}
	
	m, err = buf.Write(bb)
	if err != nil {
		t.Errorf("buf write error")
	}
	
	fmt.Println("buf read bytes: ", m)
	fmt.Println(buf.Bytes())

	result := Decode(&buf, total_len)

	fmt.Println("decode result:", result)

	if !reflect.DeepEqual(raw, result) {
		t.Errorf("decode result not equals raw data")
	}
}
