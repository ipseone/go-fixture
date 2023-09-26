package fixture

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/oklog/ulid/v2"
	"gopkg.in/yaml.v3"
)

const tomlFormat = 0
const yamlFormat = 1

// ULID is meant to be used with (*Fixture).GetField,
// and will panic if the incoming err is not nil. E.g.:
//
//	ULID(f.GetField("users", "1", "id"))
func ULID(v any, err error) ulid.ULID {
	if err != nil {
		panic(fmt.Errorf("fixture.ULID: %w", err))
	}

	return ulid.ULID(v.([16]uint8))
}

func GetDefaultValues(file string) (Table, error) {
	body, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(file))
	table := make(Table)

	switch ext {
	case ".toml":
		if err := toml.Unmarshal(body, &table); err != nil {
			return nil, fmt.Errorf("failed to unmarshal toml: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(body, &table); err != nil {
			return nil, fmt.Errorf("failed to unmarshal yaml: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported file extension: %s", ext)
	}

	return table, nil
}

func bodyFormat(ext string) (int, error) {
	switch strings.ToLower(ext) {
	case ".toml":
		return tomlFormat, nil
	case ".yaml", ".yml":
		return yamlFormat, nil
	}

	return 0, fmt.Errorf("unsupported file extension: %s", ext)
}
