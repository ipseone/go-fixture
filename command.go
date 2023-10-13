package fixture

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"text/scanner"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

type CommandFunc func(in *CommandInput) (*CommandOutput, error)

type CommandInput struct {
	Fixture *Fixture
	Table   string
	Key     string
	Field   string

	Line string
}

type CommandDependency struct {
	Label    [2]string
	Callback func() (any, error)
}

type CommandOutput struct {
	Dependencies []*CommandDependency
	IsUpdate     bool
	Value        any
}

func (in *CommandInput) ScanLine() ([]string, map[string]string, error) {
	if in.Line == "" {
		return nil, nil, nil
	}

	var args []string
	var kwargs map[string]string
	var equalsPrefix bool
	var lastTxt string

	parse := func(txt string) {
		switch {
		case txt == "=":
			equalsPrefix = true
			return
		case equalsPrefix:
			equalsPrefix = false

			if kwargs == nil {
				kwargs = make(map[string]string)
			}

			kwargs[lastTxt] = txt
			lastTxt = ""
			return
		case lastTxt != "":
			args = append(args, lastTxt)
		}

		lastTxt = txt
	}

	s := new(scanner.Scanner).Init(strings.NewReader(in.Line))

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		parse(s.TokenText())
	}

	parse("")

	return args, kwargs, nil
}

var commands = map[string]CommandFunc{
	"base64dec": base64DecodeCommand,
	"key":       keyCommand,
	"ref":       refCommand,
	"template":  templateCommand,
	"ulid":      ulidCommand,
	"uuidv4":    uuidv4Command,
}

func base64DecodeCommand(in *CommandInput) (*CommandOutput, error) {
	args, _, err := in.ScanLine()
	if err != nil {
		return nil, fmt.Errorf("failed to scan command line: %w", err)
	}

	if len(args) == 0 {
		return nil, fmt.Errorf("expected at least 1 positional argument")
	}

	encoded, err := strconv.Unquote(args[0])
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

func keyCommand(in *CommandInput) (*CommandOutput, error) {
	args, _, err := in.ScanLine()
	if err != nil {
		return nil, fmt.Errorf("failed to scan command line: %w", err)
	}

	var t string

	if len(args) > 0 {
		t = args[0]
	}

	var v any

	switch t {
	case "":
		v = in.Key
	case "int":
		v, err = strconv.Atoi(in.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to convert key to int: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported key type: %s", t)
	}

	out := &CommandOutput{
		Value: v,
	}

	return out, nil
}

func refCommand(in *CommandInput) (*CommandOutput, error) {
	fixture := in.Fixture

	args, _, err := in.ScanLine()
	if err != nil {
		return nil, fmt.Errorf("failed to scan command line: %w", err)
	}

	if len(args) < 2 {
		return nil, fmt.Errorf("expected at least 2 positional arguments")
	}

	table, key := args[0], args[1]

	var field string

	if len(args) >= 3 {
		field = args[2]
	} else if tableOptions, ok := fixture.Config.TableOptions[table]; ok && tableOptions.PrimaryKeyName != "" {
		field = tableOptions.PrimaryKeyName
	} else {
		field = fixture.Config.PrimaryKeyName
	}

	if key == "#" {
		key = in.Key
	}

	fixture.Logger.Debug().
		Str("command", "ref").
		Str("table", table).
		Str("key", key).
		Str("field", field).
		Send()

	out := &CommandOutput{
		Dependencies: []*CommandDependency{{
			Label: [2]string{table, key},
			Callback: func() (any, error) {
				return fixture.GetField(table, key, field)
			},
		}},
	}

	return out, nil
}
func templateCommand(in *CommandInput) (*CommandOutput, error) {
	fixture := in.Fixture
	templateBuf := fixture.templateBuf

	if templateBuf != nil {
		templateBuf.Reset()
	} else {
		templateBuf = new(bytes.Buffer)
		fixture.templateBuf = templateBuf
	}

	t, err := template.New("fixture").Funcs(funcMap).Parse(in.Line)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}

	if err := t.Execute(fixture.templateBuf, fixture.TemplateData); err != nil {
		return nil, fmt.Errorf("failed to execute template: %w", err)
	}

	out := &CommandOutput{
		Value: templateBuf.String(),
	}

	return out, nil
}

func ulidCommand(in *CommandInput) (*CommandOutput, error) {
	_, kwargs, err := in.ScanLine()
	if err != nil {
		return nil, fmt.Errorf("failed to scan command line: %w", err)
	}

	var fromString string

	if v, ok := kwargs["fromString"]; ok {
		fromString, err = strconv.Unquote(v)
		if err != nil {
			return nil, fmt.Errorf("failed to unquote fromString: %w", err)
		}
	}

	var ulidValue ulid.ULID

	if fromString == "" {
		ulidValue, err = ulid.New(ulid.Timestamp(time.Now().UTC()), ulid.DefaultEntropy())
		if err != nil {
			return nil, fmt.Errorf("failed to generate ULID: %w", err)
		}
	} else {
		ulidValue, err = ulid.Parse(fromString)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ulid fromString %q: %w", fromString, err)
		}
	}

	out := &CommandOutput{}

	if v, ok := kwargs["toString"]; ok && v == "true" {
		out.Value = ulidValue.String()
	} else {
		out.Value = ulidValue
	}

	return out, nil
}

// Defaults to uuid v4.
func uuidv4Command(in *CommandInput) (*CommandOutput, error) {
	v, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate uuid: %w", err)
	}

	out := &CommandOutput{
		Value: v,
	}

	return out, nil
}
