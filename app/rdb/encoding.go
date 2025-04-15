package rdb

import (
	"encoding/binary"
	"fmt"
	"io"
)

func readSize(r io.Reader) (uint64, error) {
	var firstByte [1]byte
	if _, err := io.ReadFull(r, firstByte[:]); err != nil {
		return 0, fmt.Errorf("error reading size first byte: %w", err)
	}

	// 0xC0 = 11000000
	encoding := firstByte[0] & 0xC0

	switch encoding {
	case 0x00: // 00xxxxxx - 6 bit length
		return uint64(firstByte[0] & 0x3F), nil

	case 0x40: // 01xxxxxx - 14 bit length
		var secondByte [1]byte
		if _, err := io.ReadFull(r, secondByte[:]); err != nil {
			return 0, fmt.Errorf("error reading size second byte: %w", err)
		}

		return uint64((uint16(firstByte[0]&0x3F) << 8) | uint16(secondByte[0])), nil

	case 0x80: // 10xxxxxx - 32 bit length
		var length uint32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			return 0, fmt.Errorf("error reading 32-bit length: %w", err)
		}
		return uint64(length), nil

	case 0xC0: // 11xxxxxx - Special encoding
		return 0, fmt.Errorf("special encoding 0x%02x not supported for size", firstByte[0])
	}

	return 0, fmt.Errorf("invalid size encoding: 0x%02x", firstByte[0])
}

func readString(r io.Reader) (string, error) {
	var firstByte [1]byte
	if _, err := io.ReadFull(r, firstByte[:]); err != nil {
		return "", fmt.Errorf("error reading string first byte: %w", err)
	}

	encoding := firstByte[0] & 0xC0

	switch encoding {
	case 0x00, 0x40, 0x80:
		if err := unreadByte(r, firstByte[0]); err != nil {
			return "", err
		}

		length, err := readSize(r)
		if err != nil {
			return "", fmt.Errorf("error reading string length: %w", err)
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return "", fmt.Errorf("error reading string content: %w", err)
		}

		return string(data), nil

	case 0xC0: // 11xxxxxx - Special encoding
		specialFlag := firstByte[0] & 0x3F

		switch specialFlag {
		case 0: // 8-bit integer
			var val int8
			if err := binary.Read(r, binary.LittleEndian, &val); err != nil {
				return "", fmt.Errorf("error reading 8-bit integer: %w", err)
			}
			return fmt.Sprintf("%d", val), nil

		case 1: // 16-bit integer
			var val int16
			if err := binary.Read(r, binary.LittleEndian, &val); err != nil {
				return "", fmt.Errorf("error reading 16-bit integer: %w", err)
			}
			return fmt.Sprintf("%d", val), nil

		case 2: // 32-bit integer
			var val int32
			if err := binary.Read(r, binary.LittleEndian, &val); err != nil {
				return "", fmt.Errorf("error reading 32-bit integer: %w", err)
			}
			return fmt.Sprintf("%d", val), nil

		default:
			return "", fmt.Errorf("unsupported string encoding: 0x%02x", firstByte[0])
		}
	}

	return "", fmt.Errorf("invalid string encoding: 0x%02x", firstByte[0])
}

func unreadByte(r io.Reader, b byte) error {
	if ur, ok := r.(interface{ UnreadByte() error }); ok {
		return ur.UnreadByte()
	}

	return fmt.Errorf("reader does not support UnreadByte")
}
