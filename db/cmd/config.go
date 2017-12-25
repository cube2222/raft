package main

import (
	"github.com/kelseyhightower/envconfig"
	"log"
)

type Config struct {
	ClusterAddresses []string
	ClusterSize      int `default:"3"`
	RPCPort          int `default:"8001"`
}

func LoadConfig() *Config {
	conf := Config{}
	if err := envconfig.Process("raft", &conf); err != nil {
		log.Fatal(err)
	}
	log.Printf("USING CONFIG: %+v", conf)

	return &conf
}
