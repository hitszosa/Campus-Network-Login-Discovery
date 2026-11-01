package probemqtt

import "time"

type Availability struct {
	SchemaVersion int    `json:"schema_version"`
	NodeID        string `json:"node_id"`
	ConnectionID  string `json:"connection_id"`
	Status        string `json:"status"`
	Reason        string `json:"reason"`
}

type Report struct {
	SchemaVersion   int               `json:"schema_version"`
	NodeID          string            `json:"node_id"`
	ConnectionID    string            `json:"connection_id"`
	Sequence        uint64            `json:"sequence"`
	ObservedAt      time.Time         `json:"observed_at"`
	ValidForSeconds int64             `json:"valid_for_seconds"`
	Labels          map[string]string `json:"labels"`
	Reason          string            `json:"reason"`
	ProbeDurationMS int64             `json:"probe_duration_ms"`
}

type Config struct {
	Broker         string        `yaml:"broker"`
	TopicPrefix    string        `yaml:"topic_prefix"`
	ClientID       string        `yaml:"client_id"`
	KeepAlive      time.Duration `yaml:"keep_alive"`
	PublishTimeout time.Duration `yaml:"publish_timeout"`
	MaxBackoff     time.Duration `yaml:"max_backoff"`
	UsernameEnv    string        `yaml:"username_env"`
	PasswordEnv    string        `yaml:"password_env"`
	CAFile         string        `yaml:"ca_file"`
	CertFile       string        `yaml:"cert_file"`
	KeyFile        string        `yaml:"key_file"`
}
