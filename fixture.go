package fixture

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/BurntSushi/toml"
	"github.com/rs/zerolog"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/topo"
	"gopkg.in/yaml.v3"
)

type Record map[string]any
type Table map[string]Record
type Database map[string]Table

// Writer is an interface that handles inserting or updating database records.
type Writer interface {
	Insert(f *Fixture, table, key string, record Record) error
	Update(f *Fixture, table, key string, record Record) error
}

type Fixture struct {
	Context context.Context
	Logger  *zerolog.Logger

	// Config are a set of parameters that can be reused across fixtures,
	// and should only be set once.
	Config *Config

	// The writer used for this runner.
	Writer Writer

	// The directory where fixture files are located.
	// If non-empty, will be prepended to File.
	Dir string

	// TODO: Should check for one of?
	File       string
	Body       io.Reader
	BodyFormat string

	// Database can be used to set an initial database state.
	// Any records defined in the File/Body will be merged with
	// the ones defined here.
	Database Database

	// If defined, the fixture body will be parsed as a Go text/template string
	// and executed with TemplateData as its data.
	TemplateData map[string]any
	templateBuf  *bytes.Buffer

	PrintJSON               bool
	DoNotCreateDependencies bool

	applied        bool
	cmdNameBuilder *strings.Builder
	nodeIDs        map[int64]*Node
	nodesByKey     map[[2]string]*Node
	nodeSeq        int64
	touchedNodes   map[[2]string]bool
}

var defaultLogger = zerolog.Nop()

func (f *Fixture) Applied() bool {
	return f.applied
}

func (f *Fixture) Apply() error {
	if f.Config == nil {
		f.Config = &Config{}
	}

	if err := f.Config.init(); err != nil {
		return err
	}

	if f.Writer == nil {
		return fmt.Errorf("missing writer")
	}

	if f.Context == nil {
		f.Context = context.Background()
	}

	if f.Logger == nil {
		f.Logger = &defaultLogger
	}

	f.cmdNameBuilder = new(strings.Builder)
	f.nodeIDs = make(map[int64]*Node)
	f.nodesByKey = make(map[[2]string]*Node)
	f.touchedNodes = make(map[[2]string]bool)

	if f.Database == nil {
		f.Database = make(Database)
	}

	if err := f.handleFiles(); err != nil {
		return err
	}

	// Returns a list of nodes sorted topologically, so we can range
	// over it and insert records respecting their dependencies.
	nodes, err := topo.Sort(f)
	if err != nil {
		return fmt.Errorf("failed to sort records topologically: %w", err)
	}

	for i := range nodes {
		node := nodes[i].(*Node)
		label := node.Label()
		table, key := label[0], label[1]
		record := f.Database[table][key]
		tableOptions := f.Config.TableOptions[table]

		if tableOptions != nil && tableOptions.BeforeWrite != nil {
			if err := tableOptions.BeforeWrite(f.Context, record); err != nil {
				return fmt.Errorf("failed to execute BeforeWrite func: %w", err)
			}
		}

		if err := f.Writer.Insert(f, table, key, record); err != nil {
			return fmt.Errorf("failed to insert record %q.%q: %w", table, key, err)
		}

		for label, callback := range node.callbacks {
			if err := callback(); err != nil {
				return fmt.Errorf("failed to execute callback %v: %w", label, err)
			}
		}
	}

	if f.PrintJSON {
		fjson, err := json.MarshalIndent(f.Database, "", "	")
		if err != nil {
			return fmt.Errorf("failed to marshal fixture items: %w", err)
		}

		fmt.Println(string(fjson))
	}

	f.applied = true

	return nil
}

func (f *Fixture) parseTable(table string, databaseTable Table, recursiveDatabase Database) error {
	tableOptions := f.Config.TableOptions[table]
	hasTableOptions := tableOptions != nil
	syncWrites := (f.Config.WriteMode == WriteSync && (!hasTableOptions || tableOptions.WriteMode == 0)) || (hasTableOptions && tableOptions.WriteMode == WriteSync)

	keys := make([]string, len(databaseTable))
	i := 0

	for k := range databaseTable {
		keys[i] = k
		i++
	}

	if syncWrites {
		sort.Strings(keys)
	}

	for i := range keys {
		key := keys[i]
		record := databaseTable[key]
		nodeKey := [2]string{table, key}
		node := f.GetNode(nodeKey)

		f.Logger.Debug().
			Str("table", table).
			Str("key", key).
			Send()

		if hasTableOptions {
			for k, v := range tableOptions.DefaultValues {
				if _, ok := record[k]; !ok {
					record[k] = v
				}
			}
		}

		if !f.DoNotCreateDependencies && !f.touchedNodes[nodeKey] {
			f.touchedNodes[nodeKey] = true
		}

		if syncWrites && i > 0 {
			// When writing synchronously, add the previous key (node)
			// as a dependency to ensure it is processed before this one.

			dependencyNodeKey := [2]string{table, keys[i-1]}
			dependencyNode := f.GetNode(dependencyNodeKey)

			dependencyNode.AppendFrom(node)
			node.AppendTo(dependencyNode)
		}

		for field := range record {
			value := record[field]

			// Copy to prevent closure issues.
			fieldCopy := field

			f.Logger.Debug().
				Str("field", field).
				Send()

			recordErr := func(e error) *RecordError {
				return &RecordError{
					Table: table,
					Key:   key,
					Field: field,
					Err:   e,
				}
			}

			v, err := f.parseField(
				table,
				key,
				field,
				value,
				node,
				recursiveDatabase,
				func(v any) {
					record[fieldCopy] = v
				},
			)
			if err != nil {
				return recordErr(err)
			}

			record[field] = v
		}
	}

	return nil
}

func (f *Fixture) parseField(table, key, field string, value any, node *Node, recursiveDatabase Database, updateCallback func(v any)) (any, error) {
	// Check if value is a function and replace it with its return.
	if f, ok := value.(func(string) (any, error)); ok {
		v, err := f(key)
		if err != nil {
			return nil, fmt.Errorf("failed to execute func: %w", err)
		}

		value = v
	}

	var v string

	switch t := value.(type) {
	case []any:
		for i := range t {
			// Copy to prevent closure issues.
			iCopy := i

			a, err := f.parseField(
				table,
				key,
				field+"."+strconv.Itoa(i),
				t[i],
				node,
				recursiveDatabase,
				func(v any) {
					t[iCopy] = v
				})
			if err != nil {
				return nil, fmt.Errorf("failed to parse field %s.%d: %w", field, i, err)
			}

			t[i] = a
		}
	case map[string]any:
		for k := range t {
			// Copy to prevent closure issues.
			kCopy := k

			a, err := f.parseField(
				table,
				key,
				field+"."+k,
				t[k],
				node,
				recursiveDatabase,
				func(v any) {
					t[kCopy] = v
				})
			if err != nil {
				return nil, fmt.Errorf("failed to parse field %s.%s: %w", field, k, err)
			}

			t[k] = a
		}

		return t, nil
	case string:
		if t == "" {
			return value, nil
		}

		v = t
	default:
		return value, nil
	}

	if v[0] != '=' {
		refTable, _, err := f.Config.GetReference(table, field)
		if err != nil {
			return nil, fmt.Errorf("failed to get reference for %s.%s: %w", table, field, err)
		}

		if refTable != "" {
			v = "=ref " + refTable + " " + v
		} else {
			// This can only be false if v is unchanged, meaning
			// this field is not a registered reference.
			return value, nil
		}
	}

	cmdName := new(strings.Builder)

	var breakIndex int

	for i := 1; i < len(v); i++ {
		c := v[i]

		if c == ' ' || c == '\n' || c == '\t' {
			breakIndex = i
			break
		}

		cmdName.WriteByte(c)
	}

	cmdFunc, ok := commands[cmdName.String()]
	if !ok {
		return nil, fmt.Errorf("unknown command: %s", cmdName.String())
	}

	cmdIn := &CommandInput{
		Fixture: f,
		Table:   table,
		Key:     key,
		Field:   field,
	}

	if breakIndex > 0 {
		cmdIn.Line = v[breakIndex:]
	}

	cmdOut, err := cmdFunc(cmdIn)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command %s: %w", cmdName.String(), err)
	}

	if len(cmdOut.Dependencies) == 0 {
		return cmdOut.Value, nil
	}

	for j := range cmdOut.Dependencies {
		dependency := cmdOut.Dependencies[j]
		dependencyNodeKey := cmdOut.Dependencies[j].Label
		dependencyNode := f.GetNode(dependencyNodeKey)

		if dependency.Callback != nil {
			// We add this record's callback to the dependency node so
			// it will update the record once the dependency is resolved.
			dependencyNode.callbacks = append(dependencyNode.callbacks, func() error {
				v, err := dependency.Callback()
				if err != nil {
					return err
				}

				updateCallback(v)

				return nil
			})
		}

		dependencyNode.AppendFrom(node)
		node.AppendTo(dependencyNode)

		if f.DoNotCreateDependencies || f.touchedNodes[dependencyNodeKey] {
			// The dependency has already been processed, nothing to do.
			continue
		}

		f.touchedNodes[dependencyNodeKey] = true
		depTableName, depKey := dependencyNodeKey[0], dependencyNodeKey[1]

		// Check if table exists in the database.
		if depTable, ok := f.Database[depTableName]; ok {
			if _, ok := depTable[depKey]; ok {
				// Nothing to do, continue.
				continue
			}

			// If we get here the table exists but the record (key) does not.
			// Instead of using complex logic to check if the table
			// has already been processed and act accordingly, it's easier
			// and more consistent to add it to the recursiveDatabase.
		}

		// Add table/key to post-processing.
		//
		// The recursive database contains only the tables and records required to resolve
		// dependencies, and can be merged with the main database after all tables
		// have been processed.

		if _, ok := recursiveDatabase[depTableName]; !ok {
			recursiveDatabase[depTableName] = make(Table)
		}

		recursiveDatabase[depTableName][depKey] = make(Record)
	}

	return value, nil
}

func (f *Fixture) ParseTemplate(body []byte) ([]byte, error) {
	if f.templateBuf == nil {
		f.templateBuf = new(bytes.Buffer)
	} else {
		f.templateBuf.Reset()
	}

	fixtureTemplate, err := template.New("fixture").Funcs(funcMap).Parse(string(body))
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}

	if err := fixtureTemplate.Execute(f.templateBuf, f.TemplateData); err != nil {
		return nil, fmt.Errorf("failed to execute template: %w", err)
	}

	return f.templateBuf.Bytes(), nil
}

func (f *Fixture) parseBody(format int, data []byte, v any) error {
	if f.TemplateData != nil {
		var err error

		data, err = f.ParseTemplate(data)
		if err != nil {
			return err
		}
	}

	switch format {
	case tomlFormat:
		if err := toml.Unmarshal(data, v); err != nil {
			return fmt.Errorf("failed to unmarshal toml: %w", err)
		}
	case yamlFormat:
		if err := yaml.Unmarshal(data, v); err != nil {
			return fmt.Errorf("failed to unmarshal yaml: %w", err)
		}
	default:
		// This should never happen.
		return fmt.Errorf("unsupported format: %d", format)
	}

	return nil
}

func (f *Fixture) handleTableFile(format int, name string, body []byte, recursiveDatabase Database) error {
	table := make(Table)

	if err := f.parseBody(format, body, &table); err != nil {
		return fmt.Errorf("failed to unmarshal Table: %w", err)
	}

	if err := f.parseTable(name, table, recursiveDatabase); err != nil {
		return fmt.Errorf("failed to parse table %s: %w", name, err)
	}

	f.Database[name] = table

	return nil
}

func (f *Fixture) handleDatabase(database Database) error {
	recursiveDatabase := make(Database)

	for name, table := range database {
		if err := f.parseTable(name, table, recursiveDatabase); err != nil {
			return fmt.Errorf("failed to parse table %s: %w", name, err)
		}
	}

	if len(recursiveDatabase) > 0 {
		if err := f.handleDatabase(recursiveDatabase); err != nil {
			return err
		}

		// Add recursive database to main database.
		for name := range recursiveDatabase {
			table := recursiveDatabase[name]

			t, ok := f.Database[name]
			if !ok {
				// If the table does not exist, add it.
				f.Database[name] = table
			} else {
				// Otherwise add the missing records.
				for k := range table {
					t[k] = table[k]
				}
			}
		}
	}

	return nil
}

func (f *Fixture) handleDatabaseFile(format int, body []byte) error {
	if err := f.parseBody(format, body, &f.Database); err != nil {
		return fmt.Errorf("failed to unmarshal Database: %w", err)
	}

	return f.handleDatabase(f.Database)
}

func (f *Fixture) handleFiles() error {
	if f.Body != nil {
		b, err := io.ReadAll(f.Body)
		if err != nil {
			return fmt.Errorf("failed to read fixture body: %w", err)
		}

		format, err := bodyFormat(f.BodyFormat)
		if err != nil {
			return err
		}

		return f.handleDatabaseFile(format, b)
	}

	if f.File == "" {
		if len(f.Database) > 0 {
			// Apply the pre-defined database.
			return f.handleDatabase(f.Database)
		}

		return errors.New("missing fixture body or file")
	}

	file := filepath.Join(f.Dir, f.File)

	stat, err := os.Stat(file)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if !stat.IsDir() {
		b, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read fixture file: %w", err)
		}

		format, err := bodyFormat(filepath.Ext(file))
		if err != nil {
			return err
		}

		return f.handleDatabaseFile(format, b)
	}

	dirEntries, err := os.ReadDir(file)
	if err != nil {
		return fmt.Errorf("failed to read fixture directory: %w", err)
	}

	recursiveDatabase := make(Database)

	for i := range dirEntries {
		dirEntry := dirEntries[i]
		name := dirEntry.Name()
		ext := filepath.Ext(name)

		if dirEntry.IsDir() {
			continue
		}

		format, err := bodyFormat(ext)
		if err != nil {
			continue
		}

		b, err := os.ReadFile(filepath.Join(file, name))
		if err != nil {
			return fmt.Errorf("failed to read fixture file: %w", err)
		}

		if err := f.handleTableFile(format, strings.TrimSuffix(name, ext), b, recursiveDatabase); err != nil {
			return err
		}
	}

	if len(recursiveDatabase) > 0 {
		return f.handleDatabase(recursiveDatabase)
	}

	return nil
}

var ErrDatabaseNotFound = errors.New("database not found")
var ErrTableNotFound = errors.New("table not found")
var ErrRecordNotFound = errors.New("record not found")
var ErrFieldNotFound = errors.New("field not found")

func (f *Fixture) GetField(table, key, field string) (any, error) {
	if f.Database == nil {
		return nil, ErrDatabaseNotFound
	}

	tableItem, ok := f.Database[table]
	if !ok {
		return nil, ErrTableNotFound
	}

	record, ok := tableItem[key]
	if !ok {
		return nil, ErrRecordNotFound
	}

	value, ok := record[field]
	if !ok {
		return nil, ErrFieldNotFound
	}

	return value, nil
}

func (f *Fixture) SetField(table, key, field string, value any) error {
	if f.Database == nil {
		return ErrDatabaseNotFound
	}

	database := f.Database

	tableItem, ok := database[table]
	if !ok {
		tableItem = make(Table)
		database[table] = tableItem
	}

	recordItem, ok := tableItem[key]
	if !ok {
		recordItem = make(Record)
		tableItem[key] = recordItem
	}

	recordItem[field] = value

	return nil
}

func (f *Fixture) Node(id int64) graph.Node {
	return f.nodeIDs[id]
}

func (f *Fixture) Nodes() graph.Nodes {
	l := make([]*Node, len(f.nodeIDs))

	var i int

	for _, v := range f.nodeIDs {
		l[i] = v
		i++
	}

	return &Nodes{l: l}
}

func (f *Fixture) From(id int64) graph.Nodes {
	return &Nodes{
		l: f.nodeIDs[id].from,
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

	for i := range na.from {
		if na.from[i].ID() == vid {
			return true, true
		}
	}

	for i := range nb.to {
		if nb.to[i].ID() == uid {
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
	return &Nodes{
		l: f.nodeIDs[id].to,
	}
}

func (f *Fixture) GetNode(label [2]string) *Node {
	n, ok := f.nodesByKey[label]
	if ok {
		return n
	}

	f.nodeSeq++

	n = &Node{
		id:    f.nodeSeq,
		label: label,
	}

	f.nodeIDs[f.nodeSeq] = n
	f.nodesByKey[label] = n

	return n
}
