package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// RESP protocol type identifiers
const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

const (
	CRLF = "\r\n"
)

type RESP struct {
	Type   byte
	String string
	Number int
	Array  []RESP
}

// Marshal converts a RESP object to its string representation
func (r *RESP) Marshal() string {
	switch r.Type {
	case SimpleString:
		return fmt.Sprintf("+%s\r\n", r.String)
	case Error:
		return fmt.Sprintf("-%s\r\n", r.String)
	case Integer:
		return fmt.Sprintf(":%d\r\n", r.Number)
	case BulkString:
		if r.String == "" && r.Number == -1 {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(r.String), r.String)
	case Array:
		if r.Array == nil && r.Number == -1 {
			return "*-1\r\n"
		}

		var builder strings.Builder
		builder.WriteString(fmt.Sprintf("*%d\r\n", len(r.Array)))

		for _, item := range r.Array {
			builder.WriteString(item.Marshal())
		}

		return builder.String()
	default:
		return ""
	}
}

func NewSimpleString(str string) RESP {
	return RESP{Type: SimpleString, String: str}
}

func NewError(err string) RESP {
	return RESP{Type: Error, String: err}
}

func NewInteger(num int) RESP {
	return RESP{Type: Integer, Number: num}
}

func NewBulkString(str string) RESP {
	return RESP{Type: BulkString, String: str}
}

func NewArray(items []RESP) RESP {
	return RESP{Type: Array, Array: items}
}

func NewNullBulkString() RESP {
	return RESP{Type: BulkString, Number: -1}
}

func NewNullArray() RESP {
	return RESP{Type: Array, Number: -1}
}

func Parse(reader *bufio.Reader) (RESP, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return RESP{}, err
	}

	switch prefix {
	case SimpleString:
		return parseSimpleString(reader)
	case Error:
		return parseError(reader)
	case Integer:
		return parseInteger(reader)
	case BulkString:
		return parseBulkString(reader)
	case Array:
		return parseArray(reader)
	default:
		return RESP{}, fmt.Errorf("unknown RESP type: %c", prefix)
	}
}

func parseSimpleString(reader *bufio.Reader) (RESP, error) {
	line, err := readLine(reader)
	if err != nil {
		return RESP{}, err
	}
	return NewSimpleString(line), nil
}

func parseError(reader *bufio.Reader) (RESP, error) {
	line, err := readLine(reader)
	if err != nil {
		return RESP{}, err
	}
	return NewError(line), nil
}

func parseInteger(reader *bufio.Reader) (RESP, error) {
	line, err := readLine(reader)
	if err != nil {
		return RESP{}, err
	}

	num, err := strconv.Atoi(line)
	if err != nil {
		return RESP{}, err
	}

	return NewInteger(num), nil
}

func parseBulkString(reader *bufio.Reader) (RESP, error) {
	line, err := readLine(reader)
	if err != nil {
		return RESP{}, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return RESP{}, err
	}

	if length == -1 {
		return NewNullBulkString(), nil
	}

	// Read the string content
	data := make([]byte, length)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return RESP{}, err
	}

	// Read and discard CRLF
	_, err = reader.ReadByte()
	if err != nil {
		return RESP{}, err
	}
	_, err = reader.ReadByte()
	if err != nil {
		return RESP{}, err
	}

	return NewBulkString(string(data)), nil
}

func parseArray(reader *bufio.Reader) (RESP, error) {
	line, err := readLine(reader)
	if err != nil {
		return RESP{}, err
	}

	count, err := strconv.Atoi(line)
	if err != nil {
		return RESP{}, err
	}

	if count == -1 {
		return NewNullArray(), nil
	}

	items := make([]RESP, 0, count)
	for i := 0; i < count; i++ {
		item, err := Parse(reader)
		if err != nil {
			return RESP{}, err
		}
		items = append(items, item)
	}

	return NewArray(items), nil
}

func readLine(reader *bufio.Reader) (string, error) {
	var line []byte
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return "", err
		}

		if b == '\r' {
			// Expect \n to follow
			b, err = reader.ReadByte()
			if err != nil {
				return "", err
			}
			if b != '\n' {
				return "", errors.New("protocol error: expected \\n after \\r")
			}
			break
		}

		line = append(line, b)
	}

	return string(line), nil
}
