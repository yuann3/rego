package command

import (
	"fmt"
	"path/filepath"
)

func LoadRDBFile(dir, dbfilename string) error {
	if dir == "" {
		dir = serverConfig.Dir
	}
	if dbfilename == "" {
		dbfilename = serverConfig.DBFilename
	}

	fullPath := filepath.Join(dir, dbfilename)
	fmt.Printf("Loading RDB file from %s\n", fullPath)

	err := store.LoadFromRDB(dir, dbfilename)
	if err != nil {
		return fmt.Errorf("failed to load RDB file '%s': %w", fullPath, err)
	}

	return nil
}
