package fixture

import (
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Inserter is an interface that handles inserting records into the database.
type Inserter interface {
	Insert(f *Fixture, table, key string, record map[string]interface{}) error
}

type PostgresInserter struct {
	Conn *pgxpool.Pool
	Tx   pgx.Tx
}

func (i *PostgresInserter) Insert(f *Fixture, table string, key string, record map[string]interface{}) error {
	queryFields := make([]string, len(record))
	queryValues := make([]interface{}, len(record))

	var j int

	for k, v := range record {
		queryFields[j] = k
		queryValues[j] = v
		j++
	}

	sql, args, err := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Insert(table).
		Columns(queryFields...).
		Values(queryValues...).
		Suffix("RETURNING *").
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to generate sql: %w", err)
	}

	log.Debug().
		Str("key", key).
		Str("table", table).
		Str("sql", sql).
		Interface("sql_args", args).
		Send()

	var rows pgx.Rows

	switch {
	case i.Tx != nil:
		rows, err = i.Tx.Query(f.Context, sql, args...)
	case i.Conn != nil:
		rows, err = i.Conn.Query(f.Context, sql, args...)
	default:
		return fmt.Errorf("no connection or transaction")
	}

	if err != nil {
		return fmt.Errorf("failed to insert record: %w", err)
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

type RedisInserter struct {
	Client *redis.Client
}

func (i *RedisInserter) Insert(f *Fixture, table string, key string, record map[string]interface{}) error {
	cli := i.Client

	var recordKey string

	if field, ok := record["key"]; ok {
		recordKey, ok = field.(string)
		if !ok {
			return fmt.Errorf("key field must be string")
		}
	} else {
		recordKey = fmt.Sprintf("fixture#%s", uuid.NewString())
		record["key"] = recordKey
	}

	switch table {
	case "keys":
		var args redis.SetArgs

		if field, ok := record["mode"]; ok {
			v, ok := field.(string)
			if !ok {
				return fmt.Errorf("mode field must be string")
			}

			args.Mode = v
		}

		if field, ok := record["ttl"]; ok {
			v, ok := field.(string)
			if !ok {
				return fmt.Errorf("ttl field must be string")
			}

			d, err := time.ParseDuration(v)
			if err != nil {
				return fmt.Errorf("failed to parse ttl as duration: %w", err)
			}

			args.TTL = d
		}

		if field, ok := record["expire_at"]; ok {
			v, ok := field.(time.Time)
			if !ok {
				return fmt.Errorf("expire_at field must be time.Time")
			}

			args.ExpireAt = v
		}

		if field, ok := record["keep_ttl"]; ok {
			v, ok := field.(bool)
			if !ok {
				return fmt.Errorf("keep_ttl field must be bool")
			}

			args.KeepTTL = v
		}

		_, err := cli.SetArgs(f.Context, recordKey, record["value"], args).Result()
		if err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}

	case "hashes":
		pairs, ok := record["pairs"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("pairs field must be map[string]interface{}")
		}

		values := make([]interface{}, len(pairs)*2)
		j := 0

		for k, v := range pairs {
			values[j] = k
			values[j+1] = v
			j += 2
		}

		_, err := cli.HSet(f.Context, recordKey, values...).Result()
		if err != nil {
			return fmt.Errorf("failed to set hash: %w", err)
		}
	}

	return nil
}
