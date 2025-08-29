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

const (
	RDB_OPCODE_EOF          = 0xFF
	RDB_OPCODE_SELECTDB     = 0xFE
	RDB_OPCODE_EXPIRETIME   = 0xFD
	RDB_OPCODE_EXPIRETIMEMS = 0xFC
	RDB_OPCODE_RESIZEDB     = 0xFB
	RDB_OPCODE_AUX          = 0xFA

	RDB_TYPE_STRING = 0
)

// ParseRDB loads keys from an RDB file into the provided store.
func ParseRDB(filePath string, store *KeyValueStore) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	signature := make([]byte, 9)
	if _, err := io.ReadFull(reader, signature); err != nil {
		return fmt.Errorf("failed to read RDB signature: %w", err)
	}

	if string(signature[:5]) != "REDIS" {
		return fmt.Errorf("invalid RDB signature: %s", string(signature[:5]))
	}



	for {
		typeByte, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading opcode: %w", err)
		}

		switch typeByte {
		case RDB_OPCODE_EOF:
			return nil

		case RDB_OPCODE_SELECTDB:
			_, err := readLength(reader)
			if err != nil {
				return fmt.Errorf("error reading database number: %w", err)
			}

		case RDB_OPCODE_RESIZEDB:
			_, err := readLength(reader)
			if err != nil {
				return fmt.Errorf("error reading hash table size: %w", err)
			}

			_, err = readLength(reader)
			if err != nil {
				return fmt.Errorf("error reading expire hash table size: %w", err)
			}

		case RDB_OPCODE_EXPIRETIME:
			var seconds uint32
			if err := binary.Read(reader, binary.LittleEndian, &seconds); err != nil {
				return fmt.Errorf("error reading expire time: %w", err)
			}

			expiryTime := time.Unix(int64(seconds), 0)
			if err := parseKeyValuePair(reader, store, expiryTime); err != nil {
				return err
			}

		case RDB_OPCODE_EXPIRETIMEMS:
			var ms uint64
			if err := binary.Read(reader, binary.LittleEndian, &ms); err != nil {
				return fmt.Errorf("error reading expire time ms: %w", err)
			}

			expiryTime := time.UnixMilli(int64(ms))
			if err := parseKeyValuePair(reader, store, expiryTime); err != nil {
				return err
			}

		case RDB_OPCODE_AUX:
			key, err := readString(reader)
			if err != nil {
				return fmt.Errorf("error reading AUX key: %w", err)
			}

			value, err := readString(reader)
			if err != nil {
				return fmt.Errorf("error reading AUX value: %w", err)
			}

			_ = key
			_ = value

		default:
			valueType := typeByte

			if valueType != RDB_TYPE_STRING {
				return fmt.Errorf("unsupported value type: %d", valueType)
			}

			key, err := readString(reader)
			if err != nil {
				return fmt.Errorf("error reading key: %w", err)
			}

			value, err := readString(reader)
			if err != nil {
				return fmt.Errorf("error reading value: %w", err)
			}

			store.Set(key, value, 0)
		}
	}

	return nil
}

// parseKeyValuePair reads a typed key/value with an optional expiry and stores it.
func parseKeyValuePair(reader *bufio.Reader, store *KeyValueStore, expiryTime time.Time) error {
	valueType, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("error reading value type: %w", err)
	}

    switch valueType {
    case RDB_TYPE_STRING:
        key, err := readString(reader)
        if err != nil {
            return fmt.Errorf("error reading key: %w", err)
        }

        value, err := readString(reader)
        if err != nil {
            return fmt.Errorf("error reading string value: %w", err)
        }
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

// readLength reads an encoded length from the RDB stream.
func readLength(reader *bufio.Reader) (uint64, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

    switch (b >> 6) & 0x03 {
    case 0:
        return uint64(b & 0x3F), nil

    case 1:
        second, err := reader.ReadByte()
        if err != nil {
            return 0, err
        }
        return uint64((uint16(b&0x3F) << 8) | uint16(second)), nil

    case 2:
        buf := make([]byte, 4)
        if _, err := io.ReadFull(reader, buf); err != nil {
            return 0, err
        }
        return uint64(binary.BigEndian.Uint32(buf)), nil

    case 3:
        encoding := b & 0x3F
        if encoding == 0 {
            var val int8
            if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
                return 0, err
            }
            return uint64(val), nil
        } else if encoding == 1 {
            var val int16
            if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
                return 0, err
            }
            return uint64(val), nil
        } else if encoding == 2 {
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

// readString reads an encoded string from the RDB stream.
func readString(reader *bufio.Reader) (string, error) {
    b, err := reader.ReadByte()
    if err != nil {
        return "", err
    }

    if (b >> 6) == 3 {
        encoding := b & 0x3F
        switch encoding {
        case 0:
            var val int8
            if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
                return "", err
            }
            return strconv.Itoa(int(val)), nil

        case 1:
            var val int16
            if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
                return "", err
            }
            return strconv.Itoa(int(val)), nil

        case 2:
            var val int32
            if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
                return "", err
            }
            return strconv.Itoa(int(val)), nil

		default:
			return "", fmt.Errorf("unsupported string encoding: %02x", b)
		}
	}

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
