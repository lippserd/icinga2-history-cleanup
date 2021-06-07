package command

import (
	"github.com/lippserd/icinga2-history-cleanup/pkg/config"
	"github.com/lippserd/icinga2-history-cleanup/pkg/ido"
	"go.uber.org/zap"
)

type Command struct {
	Flags  *config.Flags
	Config *config.Config
	Logger *zap.SugaredLogger
}

func New() (*Command, error) {
	flags, err := config.ParseFlags()
	if err != nil {
		return nil, err
	}

	cfg, err := config.FromYAMLFile(flags.Config)
	if err != nil {
		return nil, err
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, err
	}
	sugar := logger.Sugar()

	return &Command{
		Flags:  flags,
		Config: cfg,
		Logger: sugar,
	}, nil
}

func (c Command) Database() (*ido.Ido, error) {
	return c.Config.Database.Open(c.Logger)
}
