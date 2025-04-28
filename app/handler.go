package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Handler func(args []RESP) (RESP, []byte)

type Registry struct {
	commands   map[string]Handler
	isWriteCmd map[string]bool
}

func NewRegistry() *Registry {
	r := &Registry{
		commands:   make(map[string]Handler),
		isWriteCmd: make(map[string]bool),
	}
	r.registerCommands()
	return r
}

func (r *Registry) registerCommands() {
	r.Register("PING", pingCommand, false)
	r.Register("ECHO", echoCommand, false)
	r.Register("SET", setCommand, true)
	r.Register("GET", getCommand, false)
	r.Register("CONFIG", configCommand, false)
	r.Register("KEYS", keysCommand, false)
	r.Register("INFO", infoCommand, false)
	r.Register("REPLCONF", replconfCommand, false)
	r.Register("PSYNC", psyncCommand, false)
	r.Register("WAIT", waitCommand, false)
	r.Register("TYPE", typeCommand, false)
	r.Register("XADD", xaddCommand, true)
	r.Register("XRANGE", xrangeCommand, false)
	r.Register("XREAD", xreadCommand, false)
}

func (r *Registry) Register(name string, handler Handler, isWrite bool) {
	name = strings.ToUpper(name)
	r.commands[name] = handler
	r.isWriteCmd[name] = isWrite
}

func (r *Registry) Get(name string) (Handler, bool) {
	handler, ok := r.commands[strings.ToUpper(name)]
	return handler, ok
}

func (r *Registry) IsWriteCommand(name string) bool {
	return r.isWriteCmd[strings.ToUpper(name)]
}

func pingCommand(args []RESP) (RESP, []byte) {
	if len(args) == 0 {
		return NewSimpleString("PONG"), nil
	}
	return NewBulkString(args[0].String), nil
}

func echoCommand(args []RESP) (RESP, []byte) {
	if len(args) == 0 {
		return NewError("ERR wrong number of arguments for 'echo' command"), nil
	}
	return NewBulkString(args[0].String), nil
}

func setCommand(args []RESP) (RESP, []byte) {
	if len(args) < 2 {
		return NewError("ERR wrong number of arguments for 'set' command"), nil
	}
	key := args[0].String
	value := args[1].String
	expiry := time.Duration(0)
	var nx, xx bool
	for i := 2; i < len(args); i++ {
		option := strings.ToUpper(args[i].String)
		switch option {
		case "PX":
			if i+1 >= len(args) {
				return NewError("ERR syntax error"), nil
			}
			ms, err := strconv.ParseInt(args[i+1].String, 10, 64)
			if err != nil || ms <= 0 {
				return NewError("ERR value is not an integer or out of range"), nil
			}
			expiry = time.Duration(ms) * time.Millisecond
			i++
		case "EX":
			if i+1 >= len(args) {
				return NewError("ERR syntax error"), nil
			}
			seconds, err := strconv.ParseInt(args[i+1].String, 10, 64)
			if err != nil || seconds <= 0 {
				return NewError("ERR value is not an integer or out of range"), nil
			}
			expiry = time.Duration(seconds) * time.Second
			i++
		case "NX":
			nx = true
			if xx {
				return NewError("ERR syntax error"), nil
			}
		case "XX":
			xx = true
			if nx {
				return NewError("ERR syntax error"), nil
			}
		default:
			return NewError("ERR syntax error"), nil
		}
	}
	if nx {
		if GetStore().Exists(key) {
			return NewNullBulkString(), nil
		}
	} else if xx {
		if !GetStore().Exists(key) {
			return NewNullBulkString(), nil
		}
	}
	GetStore().Set(key, value, expiry)
	return NewSimpleString("OK"), nil
}

func getCommand(args []RESP) (RESP, []byte) {
	if len(args) != 1 {
		return NewError("ERR wrong number of arguments for 'get' command"), nil
	}
	key := args[0].String
	value, exists := GetStore().Get(key)
	if !exists {
		return NewNullBulkString(), nil
	}
	return NewBulkString(value), nil
}

func keysCommand(args []RESP) (RESP, []byte) {
	if len(args) != 1 {
		return NewError("ERR wrong number of arguments for 'keys' command"), nil
	}
	pattern := args[0].String
	allKeys := GetStore().Keys()
	var matchedKeys []string
	if pattern == "*" {
		matchedKeys = allKeys
	} else if strings.HasSuffix(pattern, "*") {
		prefix := pattern[:len(pattern)-1]
		for _, key := range allKeys {
			if strings.HasPrefix(key, prefix) {
				matchedKeys = append(matchedKeys, key)
			}
		}
	} else {
		for _, key := range allKeys {
			if key == pattern {
				matchedKeys = append(matchedKeys, key)
			}
		}
	}
	items := make([]RESP, len(matchedKeys))
	for i, key := range matchedKeys {
		items[i] = NewBulkString(key)
	}
	return NewArray(items), nil
}

func infoCommand(args []RESP) (RESP, []byte) {
	if len(args) != 1 {
		return NewError("ERR wrong number of arguments for 'info' command"), nil
	}
	if strings.ToUpper(args[0].String) != "REPLICATION" {
		return NewError("ERR only replication section is supported"), nil
	}
	role := "master"
	if GetServerConfig().IsReplica {
		role = "slave"
	}
	var info string
	if role == "master" {
		replicaCount := GetReplicaCount()
		info = fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\nconnected_slaves:%d",
			role, masterReplID, masterReplOffset, replicaCount)
	} else {
		info = fmt.Sprintf("role:%s", role)
	}
	return NewBulkString(info), nil
}

func replconfCommand(args []RESP) (RESP, []byte) {
	if len(args) == 0 {
		return NewError("ERR wrong number of arguments for 'replconf' command"), nil
	}
	subCommand := strings.ToUpper(args[0].String)
	switch subCommand {
	case "GETACK":
		offset := GetOffset()
		if GetServerConfig().IsReplica {
			if offset < 0 {
				offset = 0
			}
		}
		offsetStr := strconv.FormatInt(offset, 10)
		fmt.Printf("Replying to GETACK with offset %s\n", offsetStr)
		return NewArray([]RESP{
			NewBulkString("REPLCONF"),
			NewBulkString("ACK"),
			NewBulkString(offsetStr),
		}), nil
	case "ACK":
		if len(args) >= 2 {
			offset, err := strconv.ParseInt(args[1].String, 10, 64)
			if err == nil {
				fmt.Printf("Received ACK with offset %d\n", offset)
			}
		}
		return RESP{}, nil
	case "LISTENING-PORT", "CAPA":
		return NewSimpleString("OK"), nil
	}
	return NewSimpleString("OK"), nil
}

func psyncCommand(args []RESP) (RESP, []byte) {
	response := fmt.Sprintf("FULLRESYNC %s %d", masterReplID, masterReplOffset)
	emptyRDB := []byte{
		0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
		0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69,
		0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
		0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0,
		0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff,
		0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
	}
	rdbBytes := make([]byte, 0, len(emptyRDB)+16)
	rdbBytes = append(rdbBytes, '$')
	rdbBytes = append(rdbBytes, []byte(strconv.Itoa(len(emptyRDB)))...)
	rdbBytes = append(rdbBytes, '\r', '\n')
	rdbBytes = append(rdbBytes, emptyRDB...)
	return NewSimpleString(response), rdbBytes
}

func waitCommand(args []RESP) (RESP, []byte) {
	if len(args) != 2 {
		return NewError("ERR wrong number of arguments for 'wait' command"), nil
	}
	numReplicas, err := strconv.Atoi(args[0].String)
	if err != nil {
		return NewError("ERR value is not an integer or out of range"), nil
	}
	timeout, err := strconv.Atoi(args[1].String)
	if err != nil {
		return NewError("ERR value is not an integer or out of range"), nil
	}
	replicaConns := GetReplicaConnections()
	if len(replicaConns) == 0 {
		return NewInteger(0), nil
	}
	getAckCmd := NewArray([]RESP{
		NewBulkString("REPLCONF"),
		NewBulkString("GETACK"),
		NewBulkString("*"),
	})
	cmdBytes := []byte(getAckCmd.Marshal())
	for _, conn := range replicaConns {
		_, _ = conn.Write(cmdBytes)
	}
	endTime := time.Now().Add(time.Duration(timeout) * time.Millisecond)
	currentOffset := GetMasterOffset()
	for time.Now().Before(endTime) {
		acked := GetAcknowledgedReplicaCount(currentOffset)
		if acked >= numReplicas {
			return NewInteger(acked), nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return NewInteger(GetAcknowledgedReplicaCount(currentOffset)), nil
}

func configCommand(args []RESP) (RESP, []byte) {
	if len(args) < 1 {
		return NewError("ERR wrong number of arguments for 'config' command"), nil
	}
	sub := strings.ToUpper(args[0].String)
	if sub == "GET" {
		return configGetCommand(args[1:])
	}
	return NewError("ERR unknown subcommand '" + sub + "'. Try CONFIG GET"), nil
}

func configGetCommand(args []RESP) (RESP, []byte) {
	if len(args) < 1 {
		return NewError("ERR wrong number of arguments for 'config get' command"), nil
	}
	pattern := strings.ToLower(args[0].String)
	var pairs []RESP
	cfg := GetServerConfig()
	switch pattern {
	case "dir":
		pairs = append(pairs, NewBulkString("dir"), NewBulkString(cfg.Dir))
	case "dbfilename":
		pairs = append(pairs, NewBulkString("dbfilename"), NewBulkString(cfg.DBFilename))
	case "*":
		pairs = append(pairs, NewBulkString("dir"), NewBulkString(cfg.Dir), NewBulkString("dbfilename"), NewBulkString(cfg.DBFilename))
	default:
		return NewArray(pairs), nil
	}
	return NewArray(pairs), nil
}

func parseStreamID(id string, lastID string) (int64, int64, bool, error) {
	if id == "*" {
		ms := time.Now().UnixMilli()
		return ms, 0, true, nil
	}

	if strings.HasSuffix(id, "-*") {
		timePart := strings.TrimSuffix(id, "-*")
		ms, err := strconv.ParseInt(timePart, 10, 64)
		if err != nil {
			return 0, 0, false, fmt.Errorf("invalid millsencods part")
		}

		if ms == 0 {
			return ms, 1, true, nil
		}
		return ms, 0, true, nil
	}

	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, false, fmt.Errorf("invalid stream ID format")
	}

	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("invalid millsencods part")
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("invalid sequence part")
	}

	if ms == 0 && seq == 0 {
		return 0, 0, false, fmt.Errorf("ID must be greater than 0-0")
	}

	if lastID != "" {
		lastParts := strings.Split(lastID, "-")
		if len(lastParts) != 2 {
			return 0, 0, false, fmt.Errorf("invalid last stream ID format")
		}

		lastMs, err := strconv.ParseInt(lastParts[0], 10, 64)
		if err != nil {
			return 0, 0, false, fmt.Errorf("invalid last milliseconds part")
		}

		lastSeq, err := strconv.ParseInt(lastParts[1], 10, 64)
		if err != nil {
			return 0, 0, false, fmt.Errorf("invalid last sequence part")
		}

		if ms < lastMs || (ms == lastMs && seq <= lastSeq) {
			return 0, 0, false, fmt.Errorf("ID is not greater than last entry")
		}
	}

	return ms, seq, false, nil
}

func xaddCommand(args []RESP) (RESP, []byte) {
	if len(args) < 3 {
		return NewError("ERR wrong number of arguments for 'xadd' command"), nil
	}

	if (len(args)-2)%2 != 0 {
		return NewError("ERR wrong number of arguments for 'xadd' command"), nil
	}

	key := args[0].String
	id := args[1].String

	stream, exists := GetStore().GetStream(key)
	if !exists {
		stream = &Stream{Entries: []Entry{}}
	}

	var lastID string
	if len(stream.Entries) > 0 {
		lastID = stream.Entries[len(stream.Entries)-1].ID
	}

	ms, seq, autoSeq, err := parseStreamID(id, lastID)
	if err != nil {
		if err.Error() == "ID must be greater than 0-0" {
			return NewError("ERR The ID specified in XADD must be greater than 0-0"), nil
		} else if err.Error() == "ID is not greater than last entry" {
			return NewError("ERR The ID specified in XADD is equal or smaller than the target stream top item"), nil
		}
		return NewError("ERR invalid stream ID specified as stream command argument"), nil
	}

	// real go experience, too lazy to clean it up lol
	if autoSeq {
		if strings.HasSuffix(id, "-*") {
			if ms == 0 {
				seq = 1
			} else {
				maxSeq := int64(-1)
				for _, entry := range stream.Entries {
					entryParts := strings.Split(entry.ID, "-")
					if len(entryParts) == 2 {
						entryMs, _ := strconv.ParseInt(entryParts[0], 10, 64)
						if entryMs == ms {
							entrySeq, _ := strconv.ParseInt(entryParts[1], 10, 64)
							if entrySeq > maxSeq {
								maxSeq = entrySeq
							}
						}
					}
				}
				seq = maxSeq + 1
			}
		}

		id = fmt.Sprintf("%d-%d", ms, seq)
	}

	for _, entry := range stream.Entries {
		if entry.ID == id {
			return NewError("ERR The ID specified in XADD already exists in the target stream"), nil
		}
	}

	fields := make(map[string]string)
	for i := 2; i < len(args); i += 2 {
		fieldName := args[i].String
		fieldValue := args[i+1].String
		fields[fieldName] = fieldValue
	}

	newEntry := Entry{
		ID:     id,
		Fields: fields,
	}

	stream.Entries = append(stream.Entries, newEntry)
	GetStore().Set(key, stream, 0)

	return NewBulkString(id), nil
}

func typeCommand(args []RESP) (RESP, []byte) {
	if len(args) != 1 {
		return NewError("ERR wrong number of arguments for 'type' command"), nil
	}

	key := args[0].String
	keyType := GetStore().GetType(key)

	return NewSimpleString(keyType), nil
}

func xrangeCommand(args []RESP) (RESP, []byte) {
	if len(args) != 3 {
		return NewError("ERR wrong number of arguments for 'xrange' command"), nil
	}

	key := args[0].String
	startID := args[1].String
	endID := args[2].String

	stream, exists := GetStore().GetStream(key)
	if !exists {
		return NewArray([]RESP{}), nil
	}

	startMs, startSeq, err := parseRangeID(startID, false)
	if err != nil {
		return NewError("ERR invalid stream ID specified as stream command argument"), nil
	}

	endMs, endSeq, err := parseRangeID(endID, true)
	if err != nil {
		return NewError("ERR invalid stream ID specified as stream command argument"), nil
	}

	var results []RESP
	for _, entry := range stream.Entries {
		entryMs, entrySeq, err := splitStreamID(entry.ID)
		if err != nil {
			continue
		}

		if compareStreamIDs(startMs, startSeq, entryMs, entrySeq) <= 0 &&
			compareStreamIDs(entryMs, entrySeq, endMs, endSeq) <= 0 {

			fieldValues := make([]RESP, 0, len(entry.Fields)*2)
			for field, value := range entry.Fields {
				fieldValues = append(fieldValues, NewBulkString(field))
				fieldValues = append(fieldValues, NewBulkString(value))
			}

			entryArray := NewArray([]RESP{
				NewBulkString(entry.ID),
				NewArray(fieldValues),
			})

			results = append(results, entryArray)
		}
	}

	return NewArray(results), nil
}

func parseRangeID(id string, isEnd bool) (int64, int64, error) {
	if id == "-" {
		return 0, 0, nil
	}

	if id == "+" {
		return int64(^uint64(0) >> 1), int64(^uint64(0) >> 1), nil
	}

	parts := strings.Split(id, "-")
	if len(parts) == 1 {
		ms, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, err
		}

		if isEnd {
			return ms, int64(^uint64(0) >> 1), nil
		}
		return ms, 0, nil
	}

	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid stream ID format")
	}

	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return ms, seq, nil
}

func splitStreamID(id string) (int64, int64, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid stream ID format")
	}

	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return ms, seq, nil
}

func compareStreamIDs(ms1, seq1, ms2, seq2 int64) int {
	if ms1 < ms2 {
		return -1
	}
	if ms1 > ms2 {
		return 1
	}
	if seq1 < seq2 {
		return -1
	}
	if seq1 > seq2 {
		return 1
	}
	return 0
}

func xreadCommand(args []RESP) (RESP, []byte) {
	if len(args) < 3 {
		return NewError("ERR wrong number of arguments for 'xread' command"), nil
	}

	if strings.ToUpper(args[0].String) != "STREAMS" {
		return NewError("ERR syntax error"), nil
	}

	argsAfterStreams := args[1:]
	if len(argsAfterStreams)%2 != 0 {
		return NewError("ERR syntax error"), nil
	}

	numStreams := len(argsAfterStreams) / 2
	keys := argsAfterStreams[:numStreams]
	ids := argsAfterStreams[numStreams:]

	var results []RESP
	for i := 0; i < numStreams; i++ {
		key := keys[i].String
		startID := ids[i].String

		startMs, startSeq, err := parseRangeID(startID, false)
		if err != nil {
			return NewError("ERR invalid stream ID specified as stream command argument"), nil
		}

		stream, exists := GetStore().GetStream(key)
		if !exists {
			continue
		}

		var streamEntries []RESP
		for _, entry := range stream.Entries {
			entryMs, entrySeq, err := splitStreamID(entry.ID)
			if err != nil {
				continue
			}

			if compareStreamIDs(startMs, startSeq, entryMs, entrySeq) < 0 {
				fieldValues := make([]RESP, 0, len(entry.Fields)*2)
				for field, value := range entry.Fields {
					fieldValues = append(fieldValues, NewBulkString(field))
					fieldValues = append(fieldValues, NewBulkString(value))
				}

				entryArray := NewArray([]RESP{
					NewBulkString(entry.ID),
					NewArray(fieldValues),
				})

				streamEntries = append(streamEntries, entryArray)
			}
		}

		if len(streamEntries) > 0 {
			streamResult := NewArray([]RESP{
				NewBulkString(key),
				NewArray(streamEntries),
			})
			results = append(results, streamResult)
		}
	}

	return NewArray(results), nil
}
