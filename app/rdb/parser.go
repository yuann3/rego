package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/command"
)

const (
	rdbHeader = "REDIS0011"

	// RDB Opcodes
	opEOF          = 0xFF
	opSELECTDB     = 0xFE
	opEXPIRETIME   = 0xFD
	opEXPIRETIMEMS = 0xFC
	opRESIZEDB     = 0xFB
	opAUX          = 0xFA

	// Value types
	typeString = 0
)

func Parse(filename string, store *command.KeyValueStore) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	if err := parseHeader(reader); err != nil {
		return err
	}

	if err := parseBody(reader, store); err != nil {
		return err
	}

	return nil
}

func parseHeader(r *bufio.Reader) error {
	header := make([]byte, len(rdbHeader))
	if _, err := io.ReadFull(r, header); err != nil {
		return fmt.Errorf("error reading RDB header: %w", err)
	}

	if string(header) != rdbHeader {
		return fmt.Errorf("invalid RDB header: %s", string(header))
	}

	return nil
}

func parseBody(r *bufio.Reader, store *command.KeyValueStore) error {
	for {
		opcode, err := r.ReadByte()
		if err != nil {
			return fmt.Errorf("error reading opcode: %w", err)
		}

		switch opcode {
		case opAUX:
			if err := skipAuxField(r); err != nil {
				return err
			}

		case opSELECTDB:
			if _, err := readSize(r); err != nil {
				return fmt.Errorf("error reading database number: %w", err)
			}

		case opRESIZEDB:
			if _, err := readSize(r); err != nil {
				return fmt.Errorf("error reading hash table size: %w", err)
			}
			if _, err := readSize(r); err != nil {
				return fmt.Errorf("error reading expire hash table size: %w", err)
			}

		case opEXPIRETIME, opEXPIRETIMEMS:
			if err := parseKeyWithExpiry(r, store, opcode); err != nil {
				return err
			}

		case opEOF:
			return nil

		default:
			if err := parseKey(r, store, opcode); err != nil {
				return err
			}
		}
	}
}

func skipAuxField(r *bufio.Reader) error {
	if _, err := readString(r); err != nil {
		return fmt.Errorf("error reading AUX key: %w", err)
	}

	if _, err := readString(r); err != nil {
		return fmt.Errorf("error reading AUX value: %w", err)
	}

	return nil
}

func parseKeyWithExpiry(r *bufio.Reader, store *command.KeyValueStore, opcode byte) error {
	var expiry time.Duration

	if opcode == opEXPIRETIME {
		var seconds uint32
		if err := binary.Read(r, binary.LittleEndian, &seconds); err != nil {
			return fmt.Errorf("error reading expiry seconds: %w", err)
		}

		expiryTime := time.Unix(int64(seconds), 0)
		expiry = time.Until(expiryTime)

	} else {
		var milliseconds uint64
		if err := binary.Read(r, binary.LittleEndian, &milliseconds); err != nil {
			return fmt.Errorf("error reading expiry milliseconds: %w", err)
		}

		expiryTime := time.Unix(0, int64(milliseconds)*int64(time.Millisecond))
		expiry = time.Until(expiryTime)
	}

	if expiry <= 0 {
		if _, err := r.ReadByte(); err != nil {
			return fmt.Errorf("error reading value type: %w", err)
		}

		if _, err := readString(r); err != nil {
			return fmt.Errorf("error reading key: %w", err)
		}

		if _, err := readString(r); err != nil {
			return fmt.Errorf("error reading value: %w", err)
		}

		return nil
	}

	valueType, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("error reading value type: %w", err)
	}

	return parseKeyValue(r, store, valueType, expiry)
}

func parseKey(r *bufio.Reader, store *command.KeyValueStore, valueType byte) error {
	return parseKeyValue(r, store, valueType, 0)
}

func parseKeyValue(r *bufio.Reader, store *command.KeyValueStore, valueType byte, expiry time.Duration) error {
	if valueType != typeString {
		return fmt.Errorf("unsupported value type: %d", valueType)
	}

	key, err := readString(r)
	if err != nil {
		return fmt.Errorf("error reading key: %w", err)
	}

	value, err := readString(r)
	if err != nil {
		return fmt.Errorf("error reading value: %w", err)
	}

	store.Set(key, value, expiry)

	return nil
}
