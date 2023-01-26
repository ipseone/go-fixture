package fixture

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/scanner"
	"text/template"
	"time"

	"github.com/rs/zerolog/log"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/topo"
	"gopkg.in/yaml.v3"
)

type Fixture struct {
	Context context.Context

	// TODO: Should check for one of?
	File string
	Body io.Reader

	// The name of the field that contains the ID of the record.
	// The default value is "id".
	IDFieldName string
	IDFieldFunc func() interface{}

	// If defined, the fixture body will be parsed as a Go text/template string
	// and executed with TemplateData as its data.
	TemplateData map[string]interface{}

	Inserter Inserter

	// TimeFormat defines the format of time.Time values in the fixture file.
	// The default format is time.RFC3339.
	TimeFormat string

	PrintJSON bool

	nodeIDs    map[int64]*Record
	nodeLabels map[[2]string]*Record
	nodeSeq    int64

	items map[string]map[string]map[string]interface{}
}

func (f *Fixture) ReadFile() error {
	b, err := os.ReadFile(f.File)
	if err != nil {
		return fmt.Errorf("failed to read fixture file: %w", err)
	}

	f.Body = bytes.NewBuffer(b)

	return nil
}

func (f *Fixture) Apply() error {
	if f.Inserter == nil {
		return fmt.Errorf("missing inserter")
	}

	if f.File != "" {
		if err := f.ReadFile(); err != nil {
			return fmt.Errorf("failed to read fixture file: %w", err)
		}
	}

	body, err := io.ReadAll(f.Body)
	if err != nil {
		return fmt.Errorf("failed to read fixture body: %w", err)
	}

	if f.TemplateData != nil {
		fixtureTemplate, err := template.New("fixture").Funcs(funcMap).Parse(string(body))
		if err != nil {
			return fmt.Errorf("could not parse fixture body as template: %w", err)
		}

		buf := new(bytes.Buffer)

		if err := fixtureTemplate.Execute(buf, f.TemplateData); err != nil {
			return fmt.Errorf("could not execute template: %w", err)
		}

		// Reassign body to the template output.
		body = buf.Bytes()
	}

	fixtureFile := make(map[string]map[string]map[string]interface{})

	if err := yaml.Unmarshal(body, fixtureFile); err != nil {
		return fmt.Errorf("failed to unmarshal fixture body: %w", err)
	}

	if f.Context == nil {
		f.Context = context.Background()
	}

	if f.IDFieldName == "" {
		f.IDFieldName = "id"
	}

	if f.TimeFormat == "" {
		f.TimeFormat = time.RFC3339
	}

	f.items = fixtureFile
	f.nodeIDs = make(map[int64]*Record)
	f.nodeLabels = make(map[[2]string]*Record)

	for table, records := range f.items {
		for key, record := range records {
			recordNode := f.GetNode([2]string{table, key})

			log.Debug().
				Str("table", table).
				Str("key", key).
				Send()

			for field, value := range record {
				log.Debug().
					Str("field", field).
					Send()

				recordEr := func(e error) *RecordError {
					return &RecordError{
						Table: table,
						Key:   key,
						Field: field,
						Err:   e,
					}
				}

				switch v := value.(type) {
				case float64:
					if field == f.IDFieldName {
						record[field] = strconv.FormatFloat(v, 'g', -1, 64)
					}
				case int:
					if field == f.IDFieldName {
						record[field] = strconv.Itoa(v)
					} else {
						record[field] = int64(v)
					}
				case string:
					// Check if value should be parsed as a command.
					if v[0] != '=' {
						continue
					}

					s := new(scanner.Scanner).Init(strings.NewReader(v[1:]))
					cmdToken := s.Scan()

					if cmdToken == scanner.EOF {
						return recordEr(errors.New("expected command name, got EOF"))
					}

					cmdName := s.TokenText()
					cmdFunc, ok := commands[cmdName]
					if !ok {
						return recordEr(fmt.Errorf("unknown command: %s", cmdName))
					}

					cmdOut, err := cmdFunc(
						&CommandInput{
							Fixture: f,
							Table:   table,
							Key:     key,
							Field:   field,
							Scanner: s,
						},
					)
					if err != nil {
						return recordEr(fmt.Errorf(
							"failed to execute command %s: %w",
							cmdName,
							err,
						))
					}

					if len(cmdOut.DependsOn) > 0 {
						for j := range cmdOut.DependsOn {
							dependencyNode := f.GetNode(cmdOut.DependsOn[j])

							if dependencyNode.callbacks == nil {
								dependencyNode.callbacks = make(map[[3]string]func() (interface{}, error))
							}

							// We add this record's callback to the dependency node so
							// it will update the record once the dependency is resolved.
							dependencyNode.callbacks[[3]string{table, key, field}] = cmdOut.Callback

							dependencyNode.AppendFrom(recordNode)
							recordNode.AppendTo(dependencyNode)
						}
					} else {
						record[field] = cmdOut.Value
					}
				}
			}
		}
	}

	// Returns a list of nodes sorted topologically, so we can range
	// over it and insert records respecting their dependencies.
	nodes, err := topo.Sort(f)
	if err != nil {
		return fmt.Errorf("failed to sort records topologically: %w", err)
	}

	for i := range nodes {
		node := nodes[i].(*Record)
		label := node.Label()
		table, key := label[0], label[1]
		record := f.items[table][key]

		if err := f.Inserter.Insert(f, table, key, record); err != nil {
			return fmt.Errorf("failed to insert record: %w", err)
		}

		for label, callback := range node.callbacks {
			v, err := callback()
			if err != nil {
				return fmt.Errorf("failed to execute callback %v: %w", label, err)
			}

			callbackTable, callbackKey, callbackField := label[0], label[1], label[2]
			f.items[callbackTable][callbackKey][callbackField] = v

			log.Debug().
				Str("table", callbackTable).
				Str("key", callbackKey).
				Str("field", callbackField).
				Interface("value", v).
				Msg("callback")
		}
	}

	if f.PrintJSON {
		fjson, err := json.MarshalIndent(f.items, "", "	")
		if err != nil {
			return fmt.Errorf("failed to marshal fixture items: %w", err)
		}

		fmt.Println(string(fjson))
	}

	return nil
}

func (f *Fixture) SetRecord(table, key string, record map[string]interface{}) {
	f.items[table][key] = record
}

func (f *Fixture) GetField(table, key, field string) (interface{}, error) {
	if f.items == nil {
		return nil, errors.New("fixture not applied")
	}

	tableItem, ok := f.items[table]
	if !ok {
		return nil, fmt.Errorf("undefined table: %s", table)
	}

	record, ok := tableItem[key]
	if !ok {
		return nil, fmt.Errorf("undefined record: %s", key)
	}

	value, ok := record[field]
	if !ok {
		return nil, fmt.Errorf("undefined field: %s", field)
	}

	return value, nil
}

func (f *Fixture) Node(id int64) graph.Node {
	return f.nodeIDs[id]
}

func (f *Fixture) Nodes() graph.Nodes {
	l := make([]*Record, len(f.nodeIDs))

	var i int

	for _, v := range f.nodeIDs {
		l[i] = v
		i++
	}

	return &Records{l: l}
}

func (f *Fixture) From(id int64) graph.Nodes {
	return &Records{
		l: f.nodeIDs[id].nodeFrom,
	}
}

func (f *Fixture) edgeBetween(uid, vid int64) (bool, bool) {
	na, ok := f.nodeIDs[uid]
	if !ok {
		return false, false
	}

	nb, ok := f.nodeIDs[vid]
	if !ok {
		return false, false
	}

	for i := range na.nodeFrom {
		if na.nodeFrom[i].ID() == vid {
			return true, true
		}
	}

	for i := range nb.nodeTo {
		if nb.nodeTo[i].ID() == uid {
			return true, false
		}
	}

	return false, false
}

func (f *Fixture) HasEdgeBetween(xid, yid int64) bool {
	has, _ := f.edgeBetween(xid, yid)
	return has
}

func (f *Fixture) Edge(uid, vid int64) graph.Edge {
	has, right := f.edgeBetween(uid, vid)

	if !has {
		return nil
	}

	if right {
		return &Edge{
			to:   f.nodeIDs[vid],
			from: f.nodeIDs[uid],
		}
	}

	return &Edge{
		to:   f.nodeIDs[uid],
		from: f.nodeIDs[vid],
	}
}

func (f *Fixture) HasEdgeFromTo(uid, vid int64) bool {
	has, right := f.edgeBetween(uid, vid)

	if !has {
		return false
	}

	return right
}

func (f *Fixture) To(id int64) graph.Nodes {
	return &Records{
		l: f.nodeIDs[id].nodeTo,
	}
}

func (f *Fixture) GetNode(label [2]string) *Record {
	n, ok := f.nodeLabels[label]
	if ok {
		return n
	}

	f.nodeSeq++

	n = &Record{
		nodeID:    f.nodeSeq,
		nodeLabel: label,
	}

	f.nodeIDs[f.nodeSeq] = n
	f.nodeLabels[label] = n

	return n
}
