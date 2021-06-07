package ido

import (
	"context"
	"fmt"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/lippserd/icinga2-history-cleanup/pkg/contracts"
	"go.uber.org/zap"
	"time"
)

type Options struct {
	InstanceId int    `yaml:"instance_id" default:"1"`
	ChunkSize  int    `yaml:"chunk_size" default:"1000"`
	Prefix     string `yaml:"prefix" default:"icinga_"`
	OlderThan  string `yaml:"older_than" default:"8760h"`
}

// Ido is a wrapper around sqlx.DB with logging capabilities.
type Ido struct {
	*sqlx.DB

	Options *Options
	Logger  *zap.SugaredLogger
}

func (ido Ido) Cleanup(ctx context.Context, table *contracts.Table) error {
	ido.Logger.Info("Cleaning table " + table.Name)

	// TODO(el): Custom type w/ UnmarshalYAML().
	olderThan, err := time.ParseDuration(ido.Options.OlderThan)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("DELETE FROM %[1]s%[2]s WHERE %[3]s = ANY(ARRAY(SELECT %[3]s FROM %[1]s%[2]s WHERE instance_id = %[4]d AND %[5]s < :time LIMIT %[6]d));",
		ido.Options.Prefix, table.Name, table.PrimaryKey, ido.Options.InstanceId, table.Column, ido.Options.ChunkSize)

	ido.Logger.Infow("Performing "+query, zap.String("time", time.Now().Add(-1*olderThan).Format("2006-01-02 15:04:05")))

	stmt, err := ido.PrepareNamedContext(ctx, query)
	if err != nil {
		return err
	}

	for {
		start := time.Now()
		rs, err := stmt.ExecContext(ctx, struct {
			Time string
		}{time.Now().Add(-1 * olderThan).Format("2006-01-02 15:04:05")})
		if err != nil {
			return err
		}
		rows, err := rs.RowsAffected()
		if err != nil {
			return err
		}
		ido.Logger.Debugf("Removed %d rows from table %s%s in %s",
			rows, ido.Options.Prefix, table.Name, time.Since(start))

		if rows < int64(ido.Options.ChunkSize) {
			break
		}
	}

	return nil
}
