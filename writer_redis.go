package fixture

import (
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/redis/go-redis/v9"
)

type RedisWriter struct {
	Client *redis.Client
}

func (w *RedisWriter) Insert(f *Fixture, table string, key string, record Record) error {
	cli := w.Client

	var recordKey string

	if field, ok := record["key"]; ok {
		recordKey, ok = field.(string)
		if !ok {
			return fmt.Errorf("key field must be string")
		}
	} else {
		v, err := ulid.New(ulid.Timestamp(time.Now().UTC()), ulid.DefaultEntropy())
		if err != nil {
			return fmt.Errorf("failed to generate ULID: %w", err)
		}

		recordKey = "fixture#" + v.String()
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
		pairs, ok := record["pairs"].(Record)
		if !ok {
			return fmt.Errorf("pairs field must be FixtureRecord")
		}

		values := make([]any, len(pairs)*2)
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

func (w *RedisWriter) Update(f *Fixture, table string, key string, record Record) error {
	return nil
}
