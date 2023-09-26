package fixture

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

var pgxPool *pgxpool.Pool
var pgxPoolOnce sync.Once

func getPgxPool(t *testing.T) *pgxpool.Pool {
	pgxPoolOnce.Do(func() {
		pgConnString := os.Getenv("FIXTURE_PG_CONN_STRING")

		if pgConnString == "" {
			pgUser := os.Getenv("PGUSER")

			if pgUser == "" {
				pgUser = "postgres"
			}

			pgPassword := os.Getenv("PGPASSWORD")

			pgHost := os.Getenv("PGHOST")

			if pgHost == "" {
				pgHost = "127.0.0.1"
			}

			pgPort := os.Getenv("PGPORT")

			if pgPort == "" {
				pgPort = "5432"
			}

			pgDatabase := os.Getenv("PGDATABASE")

			if pgDatabase == "" {
				pgDatabase = "fixture"
			}

			pgSSLMode := os.Getenv("PGSSLMODE")

			if pgSSLMode == "" {
				pgSSLMode = "disable"
			}

			pgParams := os.Getenv("FIXTURE_PGPARAMS")

			pgConnString = fmt.Sprintf(
				"postgres://%s:%s@%s:%s/%s?sslmode=%s&%s",
				pgUser,
				pgPassword,
				pgHost,
				pgPort,
				pgDatabase,
				pgSSLMode,
				pgParams,
			)
		}

		p, err := pgxpool.New(context.Background(), pgConnString)
		if err != nil {
			t.Fatalf("failed to connect to database: %s", err)
		}

		pgxPool = p
	})

	return pgxPool
}

func TestMain(m *testing.M) {
	var logLevel zerolog.Level

	if v := strings.TrimSpace(os.Getenv("ZEROLOG_LEVEL")); v == "" {
		logLevel = zerolog.InfoLevel
	} else {
		l, err := zerolog.ParseLevel(v)
		if err != nil {
			panic("failed to parse ZEROLOG_LEVEL: " + err.Error())
		}

		logLevel = l
	}

	zerolog.SetGlobalLevel(logLevel)

	os.Exit(m.Run())
}

func TestFixtureApply(t *testing.T) {
	conn := getPgxPool(t)

	if _, err := conn.Exec(
		context.Background(),
		`
		CREATE TABLE IF NOT EXISTS alpha (
			id         bigserial,
			created_at timestamptz NOT NULL DEFAULT NOW(),
			updated_at timestamptz,

			text_field text,

			PRIMARY KEY (id)
		);

		CREATE TABLE IF NOT EXISTS beta (
			id         bigserial,
			created_at timestamptz NOT NULL DEFAULT NOW(),
			updated_at timestamptz,

			alpha_id   bigint NOT NULL REFERENCES alpha (id),
			text_field text,

			PRIMARY KEY (id)
		);

		CREATE TABLE IF NOT EXISTS gamma (
			id         bigserial,
			created_at timestamptz NOT NULL DEFAULT NOW(),
			updated_at timestamptz,

			beta_id    bigint NOT NULL REFERENCES beta (id),
			text_field text,

			PRIMARY KEY (id)
		);

		CREATE TABLE IF NOT EXISTS delta (
			id         bigserial,
			created_at timestamptz NOT NULL DEFAULT NOW(),
			updated_at timestamptz,

			alpha_id   bigint NOT NULL REFERENCES alpha (id),
			gamma_id   bigint NOT NULL REFERENCES gamma (id),
			text_field text,

			PRIMARY KEY (id)
		);

		CREATE TABLE IF NOT EXISTS epsilon (
			id         uuid,
			created_at timestamptz NOT NULL DEFAULT NOW(),
			updated_at timestamptz,

			text_field text,

			PRIMARY KEY (id)
		);

		CREATE TABLE IF NOT EXISTS zeta (
			zeta_id    uuid,
			created_at timestamptz NOT NULL DEFAULT NOW(),
			updated_at timestamptz,

			text_field text,

			PRIMARY KEY (zeta_id)
		);

		CREATE TABLE IF NOT EXISTS eta (
			id         uuid,
			created_at timestamptz NOT NULL DEFAULT NOW(),
			updated_at timestamptz,

			the_zeta_id uuid REFERENCES zeta (zeta_id),
			text_field  text,

			PRIMARY KEY (id)
		);

		ALTER TABLE zeta ADD COLUMN IF NOT EXISTS eta_id uuid REFERENCES eta (id);

		CREATE TABLE IF NOT EXISTS theta (
			id         uuid,
			created_at timestamptz NOT NULL DEFAULT NOW(),
			updated_at timestamptz,

			zeta_id    uuid REFERENCES zeta (zeta_id),
			eta_id     uuid REFERENCES eta (id),
			text_field text,
			fake_ref   text,

			PRIMARY KEY (id)
		);
		`,
	); err != nil {
		t.Fatalf("failed to create schema: %s", err)
	}

	postgresWriter := &PostgresWriter{Conn: conn}
	postgresOptions := &Config{
		References: map[string]string{
			"zeta_id":  "zeta",
			"eta_id":   "eta",
			"fake_ref": "fake",
		},
		TableOptions: map[string]*TableOptions{
			"beta": {
				DefaultValues: Record{
					"alpha_id": "=ref alpha #",
				},
			},
			"gamma": {
				DefaultValues: Record{
					"beta_id": "=ref beta #",
					"text_field": func(key string) (any, error) {
						return uuid.NewString(), nil
					},
				},
			},
			"z": {
				TableName:      "zeta",
				PrimaryKeyName: "zeta_id",
			},
			"theta": {
				References: map[string]string{
					"zeta_id":  "z",
					"fake_ref": "",
				},
			},
		},
	}

	redisWriter := &RedisWriter{Client: redis.NewClient(&redis.Options{})}

	testCases := []struct {
		name                    string
		fixture                 string
		database                Database
		options                 *Config
		writer                  Writer
		doNotCreateDependencies bool
		templateData            map[string]any
	}{
		{
			name:                    "ok-toml",
			fixture:                 "fixtures/ok.toml",
			options:                 postgresOptions,
			writer:                  postgresWriter,
			doNotCreateDependencies: true,
		},
		{
			name:                    "ok-toml-dir",
			fixture:                 "fixtures/ok-toml",
			options:                 postgresOptions,
			writer:                  postgresWriter,
			doNotCreateDependencies: true,
		},
		{
			name:                    "ok-yaml",
			fixture:                 "fixtures/ok.yaml",
			options:                 postgresOptions,
			writer:                  postgresWriter,
			doNotCreateDependencies: true,
		},
		{
			name:                    "ok-yaml-dir",
			fixture:                 "fixtures/ok-yaml",
			options:                 postgresOptions,
			writer:                  postgresWriter,
			doNotCreateDependencies: true,
		},
		{
			name:    "ok-database",
			options: postgresOptions,
			writer:  postgresWriter,
			database: Database{
				"alpha": {
					"1": {
						"text_field": "alpha 1",
					},
					"2": {
						"text_field": "alpha 1",
					},
					"3": {},
				},
				"beta": {
					"1": {
						"alpha_id":   "=ref alpha 1",
						"text_field": "beta 1",
					},
					"2": {
						"alpha_id":   "=ref alpha 3",
						"text_field": "beta 2",
					},
				},
				"gamma": {
					"1": {
						"beta_id":    "=ref beta 1",
						"text_field": "gamma 1",
					},
				},
				"delta": {
					"1": {
						"alpha_id":   "=ref alpha 2",
						"gamma_id":   "=ref gamma 1",
						"text_field": "delta 1",
					},
					"2": {
						"alpha_id":   "=ref alpha #",
						"gamma_id":   "=ref gamma #",
						"text_field": "delta 2",
					},
					"3": {
						"alpha_id":   "=ref alpha #",
						"gamma_id":   "=ref gamma #",
						"text_field": "delta 3",
					},
					"4": {
						"alpha_id":   "=ref alpha #",
						"gamma_id":   "=ref gamma #",
						"text_field": "delta 4",
					},
				},
				"epsilon": {
					"1": {
						"id":         "=ulid",
						"text_field": "epsilon 1",
					},
					"2": {
						"id":         "=uuidv4",
						"text_field": "epsilon 2",
					},
				},
				"z": {
					"1": {
						"zeta_id":    "=ulid",
						"text_field": "zeta 1",
					},
					"2": {
						"zeta_id":    "=ulid",
						"text_field": "zeta 2",
					},
				},
				"eta": {
					"1": {
						"id":          "=ulid",
						"the_zeta_id": "=ref z #",
						"text_field":  "eta 1",
					},
					"2": {
						"id":          "=ulid",
						"the_zeta_id": "=ref z #",
						"text_field":  "eta 2",
					},
				},
				"theta": {
					"1": {
						"id":         "=ulid",
						"zeta_id":    "1",
						"eta_id":     "1",
						"text_field": "theta 1",
						"fake_ref":   "ref 1",
					},
					"2": {
						"id":         "=ulid",
						"zeta_id":    "2",
						"eta_id":     "=ref eta 2",
						"text_field": "theta 2",
						"fake_ref":   "ref 2",
					},
				},
			},
		},
		{
			name:    "redis",
			fixture: "fixtures/redis.yaml",
			writer:  redisWriter,
		},
	}

	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.name, func(st *testing.T) {
			f := &Fixture{
				Config:                  tc.options,
				Writer:                  tc.writer,
				File:                    tc.fixture,
				Database:                tc.database,
				TemplateData:            tc.templateData,
				DoNotCreateDependencies: tc.doNotCreateDependencies,
				PrintJSON:               true,
			}

			if err := f.Apply(); err != nil {
				st.Fatalf("failed to Apply: %s", err)
			}
		})
	}
}
