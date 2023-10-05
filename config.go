package fixture

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type TableOptions struct {
	TableName      string
	PrimaryKeyName string
	References     map[string]string
	DefaultValues  Record
	BeforeWrite    func(ctx context.Context, record Record) error
}

type Config struct {
	// The default name for the primary key field.
	// This value can be overwritten by TableOptions.
	// Default: "id"
	PrimaryKeyName string

	// The default references for all tables.
	// Can be overwritten by TableOptions.
	References map[string]string

	// TableOptions can be used to set table specific options or
	// create multiple profiles for the same table. E.g.:
	//
	// 	tableOptions := map[string]*fixture.TableOptions{
	// 		"foo": {
	// 			DefaultValues: fixture.Record{
	// 				"bar": "default value",
	// 			},
	// 		},
	// 		"foo#profile2": {
	// 			TableName:     "foo",
	// 			DefaultValues: fixture.Record{
	// 				"bar": "profile2 value",
	// 			},
	// 		},
	// 	}
	TableOptions map[string]*TableOptions

	tableAliases map[string]string

	initOnce sync.Once
	initErr  error
}

func (c *Config) init() error {
	if c == nil {
		return errors.New("nil config")
	}

	c.initOnce.Do(func() {
		if c.PrimaryKeyName == "" {
			c.PrimaryKeyName = "id"
		}

		if c.TableOptions == nil {
			c.TableOptions = make(map[string]*TableOptions)
			return
		}

		c.tableAliases = make(map[string]string)

		for table := range c.TableOptions {
			options := c.TableOptions[table]

			if options.TableName != "" {
				c.tableAliases[table] = options.TableName
			}
		}
	})

	return c.initErr
}

func (c *Config) TableAlias(table string) string {
	return c.tableAliases[table]
}

var ErrPrimaryKeyUndefined = errors.New("primary key undefined")

func (c *Config) GetPrimaryKeyName(table string) (string, error) {
	options := c.TableOptions[table]

	if options != nil && options.PrimaryKeyName != "" {
		return options.PrimaryKeyName, nil
	}

	if c.PrimaryKeyName != "" {
		return c.PrimaryKeyName, nil
	}

	return "", ErrPrimaryKeyUndefined
}

// GetReference checks if the given field has a reference and returns its table and field names.
// If no reference is found, both values are empty and no error is returned.
func (c *Config) GetReference(table, field string) (string, string, error) {
	srcTableOptions := c.TableOptions[table]
	srcHasReferences := srcTableOptions != nil && len(srcTableOptions.References) > 0
	confReferences := c.References
	confHasReferences := len(confReferences) > 0
	refTable := ""

	switch {
	case srcHasReferences:
		if t, ok := srcTableOptions.References[field]; ok && t != "" {
			refTable = t
			break
		} else if ok {
			// If the table name is empty, it means the field should not be dereferenced.
			break
		}

		fallthrough
	case confHasReferences:
		if t, ok := confReferences[field]; ok {
			refTable = t
		}
	}

	if refTable == "" {
		// Set empty values to avoid checking again.
		return "", "", nil
	}

	refPrimaryKeyName, err := c.GetPrimaryKeyName(refTable)
	if err != nil {
		return "", "", fmt.Errorf("failed to get primary key name for ref table %s: %w", refTable, err)
	}

	return refTable, refPrimaryKeyName, nil
}
