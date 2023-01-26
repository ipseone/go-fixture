package fixture

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func TestFixtureApply(t *testing.T) {
	pgxConn, err := pgxpool.New(context.Background(), "postgres://rodrigobahiense:@127.0.0.1:25433/fixture?sslmode=disable")
	if err != nil {
		t.Fatalf("failed to connect to database: %s", err)
	}

	if _, err := pgxConn.Exec(
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
		`,
	); err != nil {
		t.Fatalf("failed to create schema: %s", err)
	}

	postgresInserter := &PostgresInserter{Conn: pgxConn}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	redisInserter := &RedisInserter{Client: redisClient}

	testCases := []struct {
		name         string
		fixture      string
		inserter     Inserter
		templateData map[string]interface{}
	}{
		{
			name:     "ok",
			fixture:  "fixtures/ok.yaml",
			inserter: postgresInserter,
		},
		{
			name:     "redis",
			fixture:  "fixtures/redis.yaml",
			inserter: redisInserter,
		},
	}

	for i := range testCases {
		testCase := testCases[i]

		t.Run(testCase.name, func(st *testing.T) {
			f := &Fixture{
				Inserter:     testCase.inserter,
				File:         testCase.fixture,
				TemplateData: testCase.templateData,
				PrintJSON:    true,
			}

			if err := f.Apply(); err != nil {
				st.Fatalf("failed to Apply: %s", err)
			}
		})
	}
}
