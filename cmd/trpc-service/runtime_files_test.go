package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSharedSkillReadersNeverCreateOrOverwritePackages(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "tenant", "guide", "SKILL.md")
	content := []byte("immutable skill")
	if err := ensureSkillFile(root, path, content, true); err == nil {
		t.Fatal("reader created missing package")
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatal("reader modified directory tree")
	}
	if err := ensureSkillFile(root, path, content, false); err != nil {
		t.Fatal(err)
	}
	if err := ensureSkillDirectory(root, true); err != nil {
		t.Fatal(err)
	}
	if err := ensureSkillFile(root, path, content, true); err != nil {
		t.Fatal(err)
	}
	if err := ensureSkillFile(root, path, []byte("changed"), false); err == nil {
		t.Fatal("published package overwritten")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != string(content) {
		t.Fatal("immutable package changed")
	}
}
