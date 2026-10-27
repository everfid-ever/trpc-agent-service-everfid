package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func ensureSkillDirectory(path string, readOnly bool) error {
	if readOnly {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("shared skill directory unavailable: %w", err)
		}
		if !info.IsDir() {
			return errors.New("shared skill path is not a directory")
		}
		return nil
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	return os.Chmod(path, 0o700)
}

// Only the bootstrap writer may publish immutable package content. Publication
// is atomic and never overwrites an existing version, even across processes.
func ensureSkillFile(stagingRoot, path string, content []byte, readOnly bool) error {
	check := func() error {
		existing, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(existing, content) {
			return errors.New("published skill content does not match this binary")
		}
		return nil
	}
	if err := check(); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if readOnly {
		return errors.New("shared skill package is missing; run bootstrap before starting readers")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	// Keep unfinished files outside every published package's digest root.
	publishing := filepath.Join(stagingRoot, ".publishing")
	if err := os.MkdirAll(publishing, 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(publishing, ".skill-publish-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(content); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Link(file.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return check()
}
