package config

import (
	"os"
	"reflect"

	"github.com/go-viper/mapstructure/v2"
	"github.com/goccy/go-yaml"
)

type DbType string

const (
	DEFAULT_CONFIG_FILE = "./data/config.yml"
	TYPE_SQLITE         = "sqlite"
)

type SqliteConfig struct {
	Path  string `mapstructure:"path"`
	Debug bool   `mapstructure:"debug"`
}

type Config struct {
	DbConfig interface{} `mapstructure:"db"`
	JetstreamHosts []string `mapstructure:"jetstream_hosts,omitempty"`
}

func dbConfigDecodeHook() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f.Kind() != reflect.Map {
			return data, nil
		}

		stringMap, ok := data.(map[string]interface{})
		if !ok {
			return data, nil
		}

		// Check for the "type" key
		typeVal, ok := stringMap["type"]
		if !ok {
			return data, nil
		}

		switch typeVal {
		case TYPE_SQLITE:
			var sqliteConfig SqliteConfig
			err := mapstructure.Decode(stringMap, &sqliteConfig)
			return &sqliteConfig, err
		}

		return data, nil
	}
}

func LoadConfig(path string) (*Config, error) {
	yamlData, err := os.ReadFile(path)

	if err != nil {
		return nil, err
	}

	var rawConfig map[string]interface{}
	if err := yaml.Unmarshal(yamlData, &rawConfig); err != nil {
		return nil, err
	}

	var config Config
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:     &config,
		DecodeHook: dbConfigDecodeHook(),
	})

	if err != nil {
		return nil, err
	}

	if err := decoder.Decode(rawConfig); err != nil {
		return nil, err
	}

	if config.JetstreamHosts == nil {
		config.JetstreamHosts = []string{
			"jetstream1.us-east.bsky.network",
			"jetstream2.us-east.bsky.network",
			"jetstream1.us-west.bsky.network",
			"jetstream2.us-west.bsky.network",
		}
	}

	return &config, nil
}
