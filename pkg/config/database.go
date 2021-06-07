package config

import (
	"fmt"
	"github.com/creasty/defaults"
	"github.com/jmoiron/sqlx"
	"github.com/lippserd/icinga2-history-cleanup/pkg/ido"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Database defines database client configuration.
type Database struct {
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"`
	Database    string `yaml:"database"`
	User        string `yaml:"user"`
	Password    string `yaml:"password"`
	Driver      string `yaml:"driver" default:"pgx"`
	ido.Options `,yaml:"inline"`
}

// Open prepares the DSN string and returns the call to sqlx.Open.
func (d *Database) Open(logger *zap.SugaredLogger) (*ido.Ido, error) {
	dsn := fmt.Sprintf(
		"user=%s password=%s host=%s port=%d database=%s",
		d.User, d.Password, d.Host, d.Port, d.Database)

	db, err := sqlx.Open(d.Driver, dsn)
	if err != nil {
		return nil, err
	}

	return &ido.Ido{DB: db, Options: &d.Options, Logger: logger}, nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (d *Database) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if err := defaults.Set(d); err != nil {
		return err
	}
	// Prevent recursion.
	type self Database
	if err := unmarshal((*self)(d)); err != nil {
		return err
	}

	if d.Driver == "" {
		return errors.New("driver is required")
	}

	return nil
}
