package fixture

import (
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/gorm"
)

type PostgresWriter struct {
	Conn   *pgxpool.Pool
	Tx     pgx.Tx
	GormDB *gorm.DB
}

func (w *PostgresWriter) Insert(f *Fixture, table string, key string, record Record) error {
	queryFields := make([]string, len(record))
	queryValues := make([]any, len(record))

	var j int

	for k, v := range record {
		queryFields[j] = k
		queryValues[j] = v
		j++
	}

	if v := f.Config.TableAlias(table); v != "" {
		table = v
	}

	var args []any
	var err error
	var sql string

	if len(queryFields) == 0 {
		sql = fmt.Sprintf("INSERT INTO %s DEFAULT VALUES RETURNING *", table)
	} else {
		sql, args, err = squirrel.StatementBuilder.
			PlaceholderFormat(squirrel.Dollar).
			Insert(table).
			Columns(queryFields...).
			Values(queryValues...).
			Suffix("RETURNING *").
			ToSql()
		if err != nil {
			return fmt.Errorf("failed to generate sql: %w", err)
		}
	}

	f.Logger.Debug().
		Str("key", key).
		Str("table", table).
		Str("sql", sql).
		Interface("sql_args", args).
		Send()

	if w.GormDB != nil {
		values := make(map[string]any)

		if err := w.GormDB.WithContext(f.Context).Raw(sql, args...).Find(&values).Error; err != nil {
			return fmt.Errorf("failed query gorm database: %w", err)
		}

		for k, v := range values {
			record[k] = v
		}

		return nil
	}

	var rows pgx.Rows

	switch {
	case w.Tx != nil:
		rows, err = w.Tx.Query(f.Context, sql, args...)
	case w.Conn != nil:
		rows, err = w.Conn.Query(f.Context, sql, args...)
	default:
		return fmt.Errorf("no connection or transaction")
	}

	if err != nil {
		return fmt.Errorf("failed query database: %w", err)
	}

	defer rows.Close()

	if !rows.Next() {
		return fmt.Errorf("no rows returned: %w", rows.Err())
	}

	fieldDescriptions := rows.FieldDescriptions()

	values, err := rows.Values()
	if err != nil {
		return fmt.Errorf("failed to get row values: %w", err)
	}

	for j := range values {
		record[fieldDescriptions[j].Name] = values[j]
	}

	return nil
}

func (w *PostgresWriter) Update(f *Fixture, table string, key string, record Record) error {
	queryFields := make([]string, len(record))
	queryValues := make([]any, len(record))

	var j int

	for k, v := range record {
		queryFields[j] = k
		queryValues[j] = v
		j++
	}

	sql, args, err := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Update(table).
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to generate sql: %w", err)
	}

	f.Logger.Debug().
		Str("key", key).
		Str("table", table).
		Str("sql", sql).
		Interface("sql_args", args).
		Send()

	var rows pgx.Rows

	switch {
	case w.Tx != nil:
		rows, err = w.Tx.Query(f.Context, sql, args...)
	case w.Conn != nil:
		rows, err = w.Conn.Query(f.Context, sql, args...)
	default:
		return fmt.Errorf("no connection or transaction")
	}

	if err != nil {
		return fmt.Errorf("failed query database: %w", err)
	}

	defer rows.Close()

	if !rows.Next() {
		return fmt.Errorf("no rows returned: %w", rows.Err())
	}

	fieldDescriptions := rows.FieldDescriptions()

	values, err := rows.Values()
	if err != nil {
		return fmt.Errorf("failed to get row values: %w", err)
	}

	for j := range values {
		record[fieldDescriptions[j].Name] = values[j]
	}

	return nil
}
