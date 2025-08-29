package main

// Entry represents a single stream entry.
type Entry struct {
    ID     string
    Fields map[string]string
}

// Stream holds an ordered list of entries.
type Stream struct {
    Entries []Entry
}
