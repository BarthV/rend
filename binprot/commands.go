// Copyright 2015 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package binprot

import "bytes"
import "encoding/binary"

//import "fmt"

func SetCmd(key []byte, flags, exptime, dataSize uint32) []byte {
	// opcode, keyLength, extraLength, totalBodyLength
	// key + extras + body
	totalBodyLength := len(key) + 8 + int(dataSize)
	header := MakeRequestHeader(OPCODE_SET, len(key), 8, totalBodyLength)

	reqBuf := new(bytes.Buffer)
	binary.Write(reqBuf, binary.BigEndian, header)

	binary.Write(reqBuf, binary.BigEndian, flags)
	binary.Write(reqBuf, binary.BigEndian, exptime)
	binary.Write(reqBuf, binary.BigEndian, key)

	//fmt.Printf("Set: key: %v | flags: %v | exptime: %v | totalBodyLength: %v\n", string(key), flags, exptime, totalBodyLength)
	return reqBuf.Bytes()
}

func GetCmd(key []byte) []byte {
	// opcode, keyLength, extraLength, totalBodyLength
	header := MakeRequestHeader(OPCODE_GET, len(key), 0, len(key))

	reqBuf := new(bytes.Buffer)
	binary.Write(reqBuf, binary.BigEndian, header)

	binary.Write(reqBuf, binary.BigEndian, key)

	//fmt.Printf("Get: key: %v | totalBodyLength: %v\n", string(key), len(key))
	return reqBuf.Bytes()
}

func GATCmd(key []byte, exptime uint32) []byte {
	// opcode, keyLength, extraLength, totalBodyLength
	header := MakeRequestHeader(OPCODE_GAT, len(key), 4, len(key))

	reqBuf := new(bytes.Buffer)
	binary.Write(reqBuf, binary.BigEndian, header)

	binary.Write(reqBuf, binary.BigEndian, exptime)
	binary.Write(reqBuf, binary.BigEndian, key)

	//fmt.Printf("GAT: key: %v | exptime: %v | totalBodyLength: %v\n", string(key), exptime, len(key))
	return reqBuf.Bytes()
}

func DeleteCmd(key []byte) []byte {
	// opcode, keyLength, extraLength, totalBodyLength
	header := MakeRequestHeader(OPCODE_DELETE, len(key), 0, len(key))

	reqBuf := new(bytes.Buffer)
	binary.Write(reqBuf, binary.BigEndian, header)

	binary.Write(reqBuf, binary.BigEndian, key)

	//fmt.Printf("Delete: key: %v | totalBodyLength: %v\n", string(key), len(key))
	return reqBuf.Bytes()
}

func TouchCmd(key []byte, exptime uint32) []byte {
	// opcode, keyLength, extraLength, totalBodyLength
	// key + extras + body
	totalBodyLength := len(key) + 4
	header := MakeRequestHeader(OPCODE_TOUCH, len(key), 4, totalBodyLength)

	reqBuf := new(bytes.Buffer)
	binary.Write(reqBuf, binary.BigEndian, header)

	binary.Write(reqBuf, binary.BigEndian, exptime)
	binary.Write(reqBuf, binary.BigEndian, key)

	//fmt.Printf("GAT: key: %v | exptime: %v | totalBodyLength: %v\n", string(key), exptime, totalBodyLength)
	return reqBuf.Bytes()
}
