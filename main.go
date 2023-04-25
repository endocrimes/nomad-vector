package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/alecthomas/kong"
	"github.com/endocrimes/nomad-vector/internal/allocwatcher"
	"github.com/endocrimes/nomad-vector/internal/system"
	"github.com/endocrimes/nomad-vector/internal/termination"
	"github.com/hashicorp/nomad/api"
)

var (
	Version = "dev"
)

type cLI struct {
	NomadDataDir    string `env:"NOMAD_DATA_DIR"`
	VectorConfigDir string `env:"VECTOR_CONFIG_DIR"`

	RefreshInterval time.Duration `env:"REFRESH_INTERVAL" default:"30s"`
}

func init() {
	rand.Seed(time.Now().Unix())
}

func main() {
	err := run(Version)
	if err != nil && !errors.Is(err, termination.ErrTerminated) {
		log.Fatal("Unexpected Error: ", err)
	}
	log.Println("exiting 0")
}

func run(version string) error {
	cli := cLI{}
	kong.Parse(&cli)

	ctx := context.Background()

	sys := system.New()
	defer sys.Cleanup(ctx)

	nomadClient, _ := api.NewClient(api.DefaultConfig())
	_, err := allocwatcher.LoadIntoSystem(sys, cli.RefreshInterval, make(chan *allocwatcher.AllocChangeSet, 10), nomadClient)
	if err != nil {
		return err
	}

	return sys.Run(ctx, time.Millisecond)
}
