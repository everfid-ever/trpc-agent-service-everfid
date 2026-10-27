package main

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMultinodeComposeSeparatesWritersFromReadOnlyNodes(t *testing.T) {
	data, err := os.ReadFile("../../deploy/compose/docker-compose.local.yml")
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Services map[string]struct {
			ReadOnly    bool              `yaml:"read_only"`
			Tmpfs       []string          `yaml:"tmpfs"`
			Volumes     []string          `yaml:"volumes"`
			StopGrace   string            `yaml:"stop_grace_period"`
			Environment map[string]string `yaml:"environment"`
		}
	}
	if err = yaml.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"webui-node-a", "webui-node-b", "wecom-ha-node-a", "wecom-ha-node-b"} {
		service, ok := config.Services[name]
		if !ok {
			t.Fatal(name)
		}
		if !service.ReadOnly || service.Environment["TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY"] != "true" || service.StopGrace == "" {
			t.Fatalf("node isolation missing: %s", name)
		}
		if len(service.Tmpfs) != 1 || !strings.HasPrefix(service.Tmpfs[0], "/tmp:") {
			t.Fatalf("node has no private temporary filesystem: %s", name)
		}
		for _, volume := range service.Volumes {
			if strings.Contains(volume, "/skills") && !strings.HasSuffix(volume, ":ro") {
				t.Fatalf("shared package is writable: %s", name)
			}
		}
	}
	for _, name := range []string{"webui-bootstrap", "wecom-ha-bootstrap"} {
		service := config.Services[name]
		if service.Environment["TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY"] != "false" {
			t.Fatalf("bootstrap writer disabled: %s", name)
		}
	}
}
