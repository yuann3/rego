package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	RDBHeader        = "REDIS0011"
	TypeString       = 0
	ExpireTimeMS     = 0xFC
	ExpireTimeSec    = 0xFD
	DatabaseSelectDB = 0xFE
	EOF              = 0xFF
	MetadataSection  = 0xFA
	HashTableSize    = 0xFB
)

type RDBParser struct {
	reader     *bufio.Reader
	file       *os.File
	keyStorage map[string]string
	expiryMap  map[string]time.Time
}

func NewRDBParser(filePath string) (*RDBParser, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return &RDBParser{
		reader:     bufio.NewReader(file),
		file:       file,
		keyStorage: make(map[string]string),
		expiryMap:  make(map[string]time.Time),
	}, nil
}

func (p *RDBParser) Parse() error {
	// Read the header (REDIS0011)
	header := make([]byte, len(RDBHeader))
	if _, err := io.ReadFull(p.reader, header); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	if string(header) != RDBHeader {
		return fmt.Errorf("invalid RDB header: %s", string(header))
	}

	// Process the RDB file opcodes
	for {
		opcode, err := p.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read opcode: %w", err)
		}

		switch opcode {
		case EOF:
			// End of file - ignore checksum for now
			_, err = io.ReadFull(p.reader, make([]byte, 8))
			if err != nil && err != io.EOF {
				return err
			}
			return nil

		case MetadataSection:
			// Process metadata key-value pair
			name, err := p.readString()
			if err != nil {
				return fmt.Errorf("failed to read metadata name: %w", err)
			}
			value, err := p.readString()
			if err != nil {
				return fmt.Errorf("failed to read metadata value: %w", err)
			}
			fmt.Printf("Metadata: %s = %s\n", name, value)

		case DatabaseSelectDB:
			// Select the database
			dbnum, err := p.readLength()
			if err != nil {
				return fmt.Errorf("failed to read database number: %w", err)
			}
			_ = dbnum // We only support database 0 for now

		case HashTableSize:
			// Read hash table sizes
			_, err := p.readLength() // Main hash table size
			if err != nil {
				return fmt.Errorf("failed to read hash table size: %w", err)
			}
			_, err = p.readLength() // Expires hash table size
			if err != nil {
				return fmt.Errorf("failed to read expires hash table size: %w", err)
			}

		case ExpireTimeSec:
			// Process key with seconds-based expiry
			if err := p.processKeyWithExpiry(time.Second); err != nil {
				return fmt.Errorf("failed to process key with seconds expiry: %w", err)
			}

		case ExpireTimeMS:
			// Process key with millisecond-based expiry
			if err := p.processKeyWithExpiry(time.Millisecond); err != nil {
				return fmt.Errorf("failed to process key with ms expiry: %w", err)
			}

		default:
			// This is a value type opcode
			if err := p.processKeyValue(opcode); err != nil {
				return fmt.Errorf("failed to process key-value: %w", err)
			}
		}
	}
	return nil
}

func (p *RDBParser) processKeyWithExpiry(unit time.Duration) error {
	var expiry time.Time

	if unit == time.Second {
		// Read 4-byte expiry in seconds
		buf := make([]byte, 4)
		if _, err := io.ReadFull(p.reader, buf); err != nil {
			return fmt.Errorf("failed to read expiry seconds: %w", err)
		}
		expiry = time.Unix(int64(binary.LittleEndian.Uint32(buf)), 0)
	} else {
		// Read 8-byte expiry in milliseconds
		buf := make([]byte, 8)
		if _, err := io.ReadFull(p.reader, buf); err != nil {
			return fmt.Errorf("failed to read expiry ms: %w", err)
		}
		expiry = time.UnixMilli(int64(binary.LittleEndian.Uint64(buf)))
	}

	// Read value type
	valueType, err := p.reader.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read value type: %w", err)
	}

	// Currently only supporting string type
	if valueType != TypeString {
		return fmt.Errorf("unsupported value type: %d", valueType)
	}

	// Read key and value
	key, err := p.readString()
	if err != nil {
		return fmt.Errorf("failed to read key: %w", err)
	}

	value, err := p.readString()
	if err != nil {
		return fmt.Errorf("failed to read value: %w", err)
	}

	// Only store if not expired
	if time.Now().Before(expiry) {
		p.keyStorage[key] = value
		p.expiryMap[key] = expiry
	}

	return nil
}

func (p *RDBParser) processKeyValue(valueType byte) error {
	// Currently only supporting string type
	if valueType != TypeString {
		return fmt.Errorf("unsupported value type: %d", valueType)
	}

	// Read key and value
	key, err := p.readString()
	if err != nil {
		return fmt.Errorf("failed to read key: %w", err)
	}

	value, err := p.readString()
	if err != nil {
		return fmt.Errorf("failed to read value: %w", err)
	}

	// Store with no expiry
	p.keyStorage[key] = value

	return nil
}

func (p *RDBParser) readLength() (uint64, error) {
	firstByte, err := p.reader.ReadByte()
	if err != nil {
		return 0, err
	}

	// Get the first 2 bits to determine encoding
	encodingType := (firstByte >> 6) & 0x03

	switch encodingType {
	case 0: // 00 - 6 bit length
		return uint64(firstByte & 0x3F), nil

	case 1: // 01 - 14 bit length
		secondByte, err := p.reader.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("failed to read second byte of 14-bit length: %w", err)
		}
		return uint64((uint16(firstByte&0x3F) << 8) | uint16(secondByte)), nil

	case 2: // 10 - 32 or 64 bit length
		if firstByte == 0x80 {
			// 32-bit length
			buf := make([]byte, 4)
			if _, err := io.ReadFull(p.reader, buf); err != nil {
				return 0, fmt.Errorf("failed to read 32-bit length: %w", err)
			}
			return uint64(binary.BigEndian.Uint32(buf)), nil
		} else if firstByte == 0x81 {
			// 64-bit length
			buf := make([]byte, 8)
			if _, err := io.ReadFull(p.reader, buf); err != nil {
				return 0, fmt.Errorf("failed to read 64-bit length: %w", err)
			}
			return binary.BigEndian.Uint64(buf), nil
		}
		return 0, fmt.Errorf("unsupported length format: %x", firstByte)

	case 3: // 11 - Special encodings for strings
		switch firstByte {
		case 0xC0: // 8-bit integer
			val, err := p.reader.ReadByte()
			return uint64(val), err
		case 0xC1: // 16-bit integer
			buf := make([]byte, 2)
			if _, err := io.ReadFull(p.reader, buf); err != nil {
				return 0, err
			}
			return uint64(binary.LittleEndian.Uint16(buf)), nil
		case 0xC2: // 32-bit integer
			buf := make([]byte, 4)
			if _, err := io.ReadFull(p.reader, buf); err != nil {
				return 0, err
			}
			return uint64(binary.LittleEndian.Uint32(buf)), nil
		default:
			return 0, fmt.Errorf("unsupported special encoding: %x", firstByte)
		}
	}

	return 0, fmt.Errorf("invalid length encoding: %x", firstByte)
}

func (p *RDBParser) readString() (string, error) {
	length, err := p.readLength()
	if err != nil {
		return "", fmt.Errorf("failed to read string length: %w", err)
	}

	// Handle special integer encodings (first 2 bits are 11)
	if length >= 0xC0 {
		switch length {
		case 0xC0: // 8-bit integer
			val, err := p.reader.ReadByte()
			if err != nil {
				return "", fmt.Errorf("failed to read 8-bit integer: %w", err)
			}
			return fmt.Sprintf("%d", val), nil

		case 0xC1: // 16-bit integer
			buf := make([]byte, 2)
			if _, err := io.ReadFull(p.reader, buf); err != nil {
				return "", fmt.Errorf("failed to read 16-bit integer: %w", err)
			}
			val := binary.LittleEndian.Uint16(buf)
			return fmt.Sprintf("%d", val), nil

		case 0xC2: // 32-bit integer
			buf := make([]byte, 4)
			if _, err := io.ReadFull(p.reader, buf); err != nil {
				return "", fmt.Errorf("failed to read 32-bit integer: %w", err)
			}
			val := binary.LittleEndian.Uint32(buf)
			return fmt.Sprintf("%d", val), nil

		case 0xC3: // LZF compressed string - not implemented
			return "", fmt.Errorf("LZF compressed strings not supported")

		default:
			return "", fmt.Errorf("unsupported special string encoding: %x", length)
		}
	}

	// Regular string - read the specified number of bytes
	buf := make([]byte, length)
	if _, err := io.ReadFull(p.reader, buf); err != nil {
		return "", fmt.Errorf("failed to read string content of length %d: %w", length, err)
	}

	return string(buf), nil
}

func (p *RDBParser) GetData() (map[string]string, map[string]time.Time, error) {
	if err := p.Parse(); err != nil {
		return nil, nil, err
	}
	return p.keyStorage, p.expiryMap, nil
}

func (p *RDBParser) Close() error {
	if p.file != nil {
		return p.file.Close()
	}
	return nil
}

// LoadRDBFile loads keys and expiry times from an RDB file
func LoadRDBFile(dir, filename string) (map[string]string, map[string]time.Time, error) {
	path := filepath.Join(dir, filename)

	// If file doesn't exist, return empty maps
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return make(map[string]string), make(map[string]time.Time), nil
	}

	// Create and use the parser
	parser, err := NewRDBParser(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create parser: %w", err)
	}
	defer parser.Close()

	// Parse and return the data
	return parser.GetData()
}
