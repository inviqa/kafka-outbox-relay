package config

import (
	"os"
	"reflect"
	"testing"
	"time"
)

func TestNewConfig(t *testing.T) {
	os.Args = nil

	tests := []struct {
		name    string
		want    *Config
		wantErr bool
		env     map[string]string
	}{
		{
			name:    "illegal DB driver returns error",
			want:    nil,
			wantErr: true,
			env: getEnvVars(map[string]string{
				"DB_DRIVER": "foo",
			}),
		},
		{
			name: "valid configuration",
			want: &Config{
				SkipMigrations:       true,
				DBHost:               "host",
				DBPort:               123,
				DBUser:               "joe",
				DBPass:               "passw0rd",
				DBName:               "db-name",
				DBDriver:             Postgres,
				DBOutboxTable:        "kafka_outbox",
				KafkaHost:            []string{"kafka"},
				KafkaPublishAttempts: 5,
				WriteConcurrency:     16,
				PollFrequencyMs:      1000,
				SidecarProxyUrl:      "http://127.0.0.1:15000",
				BatchSize:            10,
				RunOptimize:          true,
			},
			env: getEnvVars(map[string]string{
				"SKIP_MIGRATIONS":   "true",
				"DB_DRIVER":         "postgres",
				"WRITE_CONCURRENCY": "16",
				"POLL_FREQUENCY_MS": "1000",
				"BATCH_SIZE":        "10",
				"RUN_OPTIMIZE":      "true",
			}),
		},
		{
			name: "migrations are disabled by default",
			want: &Config{
				SkipMigrations:       false,
				DBHost:               "host",
				DBPort:               123,
				DBDriver:             MySQL,
				DBOutboxTable:        "kafka_outbox",
				DBUser:               "joe",
				DBPass:               "passw0rd",
				DBName:               "db-name",
				KafkaHost:            []string{"kafka"},
				KafkaPublishAttempts: 5,
				WriteConcurrency:     1,
				PollFrequencyMs:      500,
				SidecarProxyUrl:      "http://127.0.0.1:15000",
				BatchSize:            250,
			},
			env: getRequiredEnvVars(),
		},
	}
	for _, tt := range tests {
		for k, v := range tt.env {
			os.Setenv(k, v)
		}

		t.Run(tt.name, func(t *testing.T) {
			got, err := NewConfig()
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConfig() error %v is not what we expected: %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewConfig() = %#v, want %#v", got, tt.want)
			}
		})
		os.Clearenv()
	}
}

func TestConfig_GetDSN(t *testing.T) {
	type fields struct {
		DBHost            string
		DBPort            uint32
		DBUser            string
		DBPass            string
		DBName            string
		DBDriver          DbDriver
		TLSEnable         bool
		TLSSkipVerifyPeer bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "generated DSN for mysql driver",
			fields: fields{
				DBHost:            "host",
				DBPort:            3306,
				DBUser:            "user",
				DBPass:            "pass",
				DBName:            "db-name",
				DBDriver:          "mysql",
				TLSEnable:         true,
				TLSSkipVerifyPeer: true,
			},
			want: "user:pass@tcp(host:3306)/db-name?parseTime=true&tls=skip-verify&multiStatements=true",
		},
		{
			name: "generated DSN for postgres driver",
			fields: fields{
				DBHost:            "host",
				DBPort:            5432,
				DBUser:            "user",
				DBPass:            "pass",
				DBName:            "db-name",
				DBDriver:          "postgres",
				TLSEnable:         true,
				TLSSkipVerifyPeer: false,
			},
			want: "postgres://user:pass@host:5432/db-name?sslmode=verify-full",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				DBHost:            tt.fields.DBHost,
				DBPort:            tt.fields.DBPort,
				DBUser:            tt.fields.DBUser,
				DBPass:            tt.fields.DBPass,
				DBName:            tt.fields.DBName,
				DBDriver:          tt.fields.DBDriver,
				TLSEnable:         tt.fields.TLSEnable,
				TLSSkipVerifyPeer: tt.fields.TLSSkipVerifyPeer,
			}
			if got := c.GetDSN(); got != tt.want {
				t.Errorf("GetDSN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_GetPollIntervalDurationInMs(t *testing.T) {
	tests := []struct {
		name     string
		interval int
		want     time.Duration
	}{
		{
			name:     "600ms interval",
			interval: 600,
			want:     time.Duration(600) * time.Millisecond,
		},
		{
			name:     "100ms interval",
			interval: 100,
			want:     time.Duration(100) * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				PollFrequencyMs: tt.interval,
			}
			if got := c.GetPollIntervalDurationInMs(); got != tt.want {
				t.Errorf("GetPollIntervalDurationInMs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_GetDependencySystemAddresses(t *testing.T) {
	type fields struct {
		DBHost    string
		DBPort    uint32
		KafkaHost []string
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			name: "kafka hosts",
			fields: fields{
				KafkaHost: []string{"kafka", "kafka2"},
			},
			want: []string{"kafka", "kafka2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				DBHost:    tt.fields.DBHost,
				DBPort:    tt.fields.DBPort,
				KafkaHost: tt.fields.KafkaHost,
			}
			if got := c.GetDependencySystemAddresses(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetDependencySystemAddresses() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDbDriver_String(t *testing.T) {
	if got := Postgres.String(); got != "postgres" {
		t.Errorf("expected 'postgres' but got '%s'", got)
	}

	if got := MySQL.String(); got != "mysql" {
		t.Errorf("expected 'postgres' but got '%s'", got)
	}
}

func TestDbDriver_Postgres(t *testing.T) {
	if got := Postgres.Postgres(); got == false {
		t.Error("expected true but got false")
	}

	if got := Postgres.MySQL(); got == true {
		t.Error("expected false but got true")
	}
}

func TestDbDriver_MySQL(t *testing.T) {
	if got := MySQL.MySQL(); got == false {
		t.Error("expected true but got false")
	}

	if got := MySQL.Postgres(); got == true {
		t.Error("expected false but got true")
	}
}

func getEnvVars(overrides map[string]string) map[string]string {
	vars := getRequiredEnvVars()
	for k, v := range overrides {
		vars[k] = v
	}

	return vars
}

func getRequiredEnvVars() map[string]string {
	return map[string]string{
		"DB_HOST":                "host",
		"DB_PORT":                "123",
		"DB_USER":                "joe",
		"DB_PASS":                "passw0rd",
		"DB_NAME":                "db-name",
		"DB_DRIVER":              "mysql",
		"KAFKA_HOST":             "kafka",
		"KAFKA_PUBLISH_ATTEMPTS": "5",
		"SIDECAR_PROXY_URL":      "http://127.0.0.1:15000",
	}
}
