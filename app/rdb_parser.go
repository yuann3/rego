package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

// RDB File format constants
const (
	RDB_OPCODE_EOF          = 0xFF
	RDB_OPCODE_SELECTDB     = 0xFE
	RDB_OPCODE_EXPIRETIME   = 0xFD
	RDB_OPCODE_EXPIRETIMEMS = 0xFC
	RDB_OPCODE_RESIZEDB     = 0xFB
	RDB_OPCODE_AUX          = 0xFA

	RDB_TYPE_STRING = 0
)

func ParseRDB(filePath string, store *KeyValueStore) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Read Redis signature and version (9 bytes - "REDIS0011")
	signature := make([]byte, 9)
	if _, err := io.ReadFull(reader, signature); err != nil {
		return fmt.Errorf("failed to read RDB signature: %w", err)
	}

	if string(signature[:5]) != "REDIS" {
		return fmt.Errorf("invalid RDB signature: %s", string(signature[:5]))
	}

	fmt.Println("Valid RDB file detected, parsing...")

	// Parse the RDB file
	for {
		// Read type byte
		typeByte, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading opcode: %w", err)
		}

		switch typeByte {
		case RDB_OPCODE_EOF:
			fmt.Println("Reached end of RDB file")
			return nil

		case RDB_OPCODE_SELECTDB:
			// Select database - read the database number
			_, err := readLength(reader)
			if err != nil {
				return fmt.Errorf("error reading database number: %w", err)
			}

		case RDB_OPCODE_RESIZEDB:
			// Resize database - read the hash table size and expire hash table size
			_, err := readLength(reader)
			if err != nil {
				return fmt.Errorf("error reading hash table size: %w", err)
			}

			_, err = readLength(reader)
			if err != nil {
				return fmt.Errorf("error reading expire hash table size: %w", err)
			}

		case RDB_OPCODE_EXPIRETIME:
			// Expire time in seconds
			var seconds uint32
			if err := binary.Read(reader, binary.LittleEndian, &seconds); err != nil {
				return fmt.Errorf("error reading expire time: %w", err)
			}

			expiryTime := time.Unix(int64(seconds), 0)
			if err := parseKeyValuePair(reader, store, expiryTime); err != nil {
				return err
			}

		case RDB_OPCODE_EXPIRETIMEMS:
			// Expire time in milliseconds
			var ms uint64
			if err := binary.Read(reader, binary.LittleEndian, &ms); err != nil {
				return fmt.Errorf("error reading expire time ms: %w", err)
			}

			expiryTime := time.UnixMilli(int64(ms))
			if err := parseKeyValuePair(reader, store, expiryTime); err != nil {
				return err
			}

		case RDB_OPCODE_AUX:
			// Skip auxiliary field
			key, err := readString(reader)
			if err != nil {
				return fmt.Errorf("error reading AUX key: %w", err)
			}

			value, err := readString(reader)
			if err != nil {
				return fmt.Errorf("error reading AUX value: %w", err)
			}

			fmt.Printf("AUX: %s = %s\n", key, value)

		default:
			// This is a key-value type directly
			valueType := typeByte

			// Check if it's a valid value type
			if valueType != RDB_TYPE_STRING {
				return fmt.Errorf("unsupported value type: %d", valueType)
			}

			// Read key
			key, err := readString(reader)
			if err != nil {
				return fmt.Errorf("error reading key: %w", err)
			}

			// Read value
			value, err := readString(reader)
			if err != nil {
				return fmt.Errorf("error reading value: %w", err)
			}

			fmt.Printf("Loaded key: %s with value: %s\n", key, value)
			store.Set(key, value, 0)
		}
	}

	return nil
}

func parseKeyValuePair(reader *bufio.Reader, store *KeyValueStore, expiryTime time.Time) error {
	// Read value type
	valueType, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("error reading value type: %w", err)
	}

	// Process based on value type
	switch valueType {
	case RDB_TYPE_STRING:
		// Read key
		key, err := readString(reader)
		if err != nil {
			return fmt.Errorf("error reading key: %w", err)
		}

		// Read value
		value, err := readString(reader)
		if err != nil {
			return fmt.Errorf("error reading string value: %w", err)
		}

		fmt.Printf("Loaded key: %s with value: %s\n", key, value)

		// Set with expiry if needed
		if !expiryTime.IsZero() {
			duration := expiryTime.Sub(time.Now())
			if duration > 0 {
				store.Set(key, value, duration)
			}
		} else {
			store.Set(key, value, 0)
		}

	default:
		return fmt.Errorf("unsupported value type: %d", valueType)
	}

	return nil
}

// readLength reads a length encoded integer from the RDB file
func readLength(reader *bufio.Reader) (uint64, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

	// Check the first 2 bits to determine the encoding
	switch (b >> 6) & 0x03 {
	case 0: // 00xxxxxx - 6 bit length
		return uint64(b & 0x3F), nil

	case 1: // 01xxxxxx - 14 bit length (6 bits + 8 bits)
		second, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return uint64((uint16(b&0x3F) << 8) | uint16(second)), nil

	case 2: // 10xxxxxx - 32 bit length
		buf := make([]byte, 4)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return 0, err
		}
		return uint64(binary.BigEndian.Uint32(buf)), nil

	case 3: // 11xxxxxx - Special encoding
		encoding := b & 0x3F
		// Handle special encodings
		if encoding == 0 {
			// Read 8-bit integer
			var val int8
			if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
				return 0, err
			}
			return uint64(val), nil
		} else if encoding == 1 {
			// Read 16-bit integer
			var val int16
			if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
				return 0, err
			}
			return uint64(val), nil
		} else if encoding == 2 {
			// Read 32-bit integer
			var val int32
			if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
				return 0, err
			}
			return uint64(val), nil
		} else {
			return 0, fmt.Errorf("unsupported special encoding: %02x", b)
		}
	}

	return 0, fmt.Errorf("invalid length encoding")
}

// readString reads an encoded string from the RDB file
func readString(reader *bufio.Reader) (string, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return "", err
	}

	// Check special encodings for strings
	if (b >> 6) == 3 { // 11xxxxxx
		encoding := b & 0x3F
		switch encoding {
		case 0: // 8-bit integer
			var val int8
			if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
				return "", err
			}
			return strconv.Itoa(int(val)), nil

		case 1: // 16-bit integer
			var val int16
			if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
				return "", err
			}
			return strconv.Itoa(int(val)), nil

		case 2: // 32-bit integer
			var val int32
			if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
				return "", err
			}
			return strconv.Itoa(int(val)), nil

		default:
			return "", fmt.Errorf("unsupported string encoding: %02x", b)
		}
	}

	// Otherwise, put the byte back and read as a length-prefixed string
	if err := reader.UnreadByte(); err != nil {
		return "", err
	}

	length, err := readLength(reader)
	if err != nil {
		return "", err
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}
