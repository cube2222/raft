package main

import (
	"github.com/kelseyhightower/envconfig"
	"log"
)

type Config struct {
	ClusterAddress   string
	BootstrapNodes   int
}

func LoadConfig() *Config {
	conf := Config{}
	if err := envconfig.Process("raft", &conf); err != nil {
		log.Fatal(err)
	}

	return &conf
}
