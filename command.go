package fixture

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"text/scanner"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
)

type CommandInput struct {
	Fixture *Fixture
	Table   string
	Key     string
	Field   string

	Scanner *scanner.Scanner
}

type CommandOutput struct {
	Callback  func() (interface{}, error)
	DependsOn [][2]string
	Value     interface{}
}

var commands = map[string]func(in *CommandInput) (*CommandOutput, error){
	"base64dec": base64DecodeCommand,
	"ref":       ref,
	"ulid":      ulidCommand,
	"uuid":      uuidCommand,
}

func base64DecodeCommand(in *CommandInput) (*CommandOutput, error) {
	cmdScanner := in.Scanner
	token := cmdScanner.Scan()

	if token == scanner.EOF {
		return nil, fmt.Errorf("expected base64 string at position 1")
	}

	encoded, err := strconv.Unquote(cmdScanner.TokenText())
	if err != nil {
		return nil, fmt.Errorf("failed to unquote base64 string: %w", err)
	}

	b, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 string: %w", err)
	}

	out := &CommandOutput{
		Value: b,
	}

	return out, nil
}

func ref(in *CommandInput) (*CommandOutput, error) {
	fixture := in.Fixture
	cmdScanner := in.Scanner
	tableToken := cmdScanner.Scan()

	if tableToken == scanner.EOF {
		return nil, fmt.Errorf("expected table name at position 1")
	}

	table := cmdScanner.TokenText()
	idToken := cmdScanner.Scan()

	if idToken == scanner.EOF {
		return nil, fmt.Errorf("expected id value at position 2")
	}

	key := cmdScanner.TokenText()
	fieldToken := cmdScanner.Scan()

	var field string

	if fieldToken == scanner.EOF {
		field = fixture.IDFieldName
	} else {
		field = cmdScanner.TokenText()
	}

	log.Debug().
		Str("command", "get").
		Str("table", table).
		Str("key", key).
		Str("field", field).
		Send()

	out := &CommandOutput{
		Callback: func() (interface{}, error) {
			return fixture.GetField(table, key, field)
		},
		DependsOn: [][2]string{{table, key}},
	}

	return out, nil
}

func ulidCommand(in *CommandInput) (*CommandOutput, error) {
	out := &CommandOutput{
		Value: ulid.Make(),
	}

	return out, nil
}

// Defaults to uuid v4.
func uuidCommand(in *CommandInput) (*CommandOutput, error) {
	out := &CommandOutput{
		Value: uuid.New(),
	}

	return out, nil
}
