package compressor

import "io"


func Encode(w io.Writer, n uint32) uint {
	bytes := 0
	switch {
	case n < 128:
		bytes = 1
		n = (n << 1)
	case n < 16384:
		bytes = 2
		n = (n << 2) | 1
	case n < 2097152:
		bytes = 3
		n = (n << 3) | 3
	default:
		bytes = 4
		n = (n << 4) | 7
	}

	d := [4]byte{
		byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
	}

	len, err := w.Write(d[:bytes])
	if err != nil {
		panic(err)
	}	

	return uint(len)
}

func Decode(r io.Reader, len uint) []uint32 {
	result := make([]uint32, 0)
	p := make([]byte, len)
	_, err := r.Read(p)
	if err != nil {
		panic(err)
	}	

	var index uint = 0
	for {
		if index >= len {
			break
		}
		
		var bytes uint = 0
		if (uint(p[index]) & 0x1) == 0 {
			bytes = 1
		} else if (uint(p[index]) & 0x2) == 0 {
			bytes = 2
		} else if (uint(p[index]) & 0x4) == 0 {
			bytes = 3
		} else if (uint(p[index]) & 0x8) == 0 {
			bytes = 4
		}

	
		var value uint32 = 0
		var i uint = 0
		for ; i < bytes; i++ {
			value |= (uint32(p[index+i]) << uint(8*i))
		}
		
		value >>= bytes
		result = append(result, value)

		index += bytes
	}

	return result
}
