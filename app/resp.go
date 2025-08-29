package main

import (
    "bufio"
    "errors"
    "fmt"
    "io"
    "strconv"
    "strings"
)

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

// RESP represents a value encoded using the Redis Serialization Protocol.
type RESP struct {
    Type   byte
    String string
    Number int
    Array  []RESP
}

// Marshal converts a RESP value to its wire-format string.
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

// NewSimpleString creates a RESP simple string.
func NewSimpleString(str string) RESP {
    return RESP{Type: SimpleString, String: str}
}

// NewError creates a RESP error string.
func NewError(err string) RESP {
    return RESP{Type: Error, String: err}
}

// NewInteger creates a RESP integer.
func NewInteger(num int) RESP {
    return RESP{Type: Integer, Number: num}
}

// NewBulkString creates a RESP bulk string.
func NewBulkString(str string) RESP {
    return RESP{Type: BulkString, String: str}
}

// NewArray creates a RESP array.
func NewArray(items []RESP) RESP {
    return RESP{Type: Array, Array: items}
}

// NewNullBulkString creates a RESP null bulk string.
func NewNullBulkString() RESP {
    return RESP{Type: BulkString, Number: -1}
}

// NewNullArray creates a RESP null array.
func NewNullArray() RESP {
    return RESP{Type: Array, Number: -1}
}

// Parse reads a RESP value from a buffered reader.
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

// parseSimpleString reads a simple string value.
func parseSimpleString(reader *bufio.Reader) (RESP, error) {
    line, err := readLine(reader)
    if err != nil {
        return RESP{}, err
    }
    return NewSimpleString(line), nil
}

// parseError reads an error string value.
func parseError(reader *bufio.Reader) (RESP, error) {
    line, err := readLine(reader)
    if err != nil {
        return RESP{}, err
    }
    return NewError(line), nil
}

// parseInteger reads an integer value.
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

// parseBulkString reads a bulk string value.
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

    data := make([]byte, length)
    _, err = io.ReadFull(reader, data)
    if err != nil {
        return RESP{}, err
    }

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

// parseArray reads an array value.
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
    for range count {
        item, err := Parse(reader)
        if err != nil {
            return RESP{}, err
        }
        items = append(items, item)
    }

    return NewArray(items), nil
}

// readLine reads a single line terminated by CRLF.
func readLine(reader *bufio.Reader) (string, error) {
    var line []byte
    for {
        b, err := reader.ReadByte()
        if err != nil {
            return "", err
        }

        if b == '\r' {
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
