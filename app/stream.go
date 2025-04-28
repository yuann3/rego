package main

type Entry struct {
	ID     string
	Fields map[string]string
}

type Stream struct {
	Entries []Entry
}
