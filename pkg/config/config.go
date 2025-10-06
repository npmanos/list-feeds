package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/goccy/go-yaml"
)

type ServiceConfig struct {
	Host       string  `mapstructure:"host"`
	ServiceDID string  `mapstructure:"service_did,omitempty"`
	MaxAgeDays float32 `mapstructure:"max_age"`
}

type DbType string

const (
	DEFAULT_CONFIG_FILE = "./data/config.yml"
	TYPE_SQLITE         = "sqlite"
)

type SqliteConfig struct {
	Path  string `mapstructure:"path"`
	Debug bool   `mapstructure:"debug"`
}

type ListConfig struct {
	URI string `mapstructure:"uri"`
	did string
}

func (lc *ListConfig) DID() (string, error) {
	if lc.did == "" {
		did, _ := strings.CutPrefix(lc.URI, "at://")
		did = strings.Split(did, "/")[0]

		if !strings.HasPrefix(did, "did:") {
			return "", fmt.Errorf("couldn't find a valid DID in list URI %s", lc.URI)
		}

		lc.did = did
	}

	return lc.did, nil
}

type Config struct {
	ServiceConfig  ServiceConfig `mapstructure:"service"`
	JetstreamHosts []string      `mapstructure:"jetstream_hosts,omitempty"`
	DbConfig       interface{}   `mapstructure:"db"`
	ListConfigs    []ListConfig  `mapstructure:"lists"`
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

	if config.ServiceConfig.ServiceDID == "" {
		config.ServiceConfig.ServiceDID = fmt.Sprintf("did:web:%s", config.ServiceConfig.Host)
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
