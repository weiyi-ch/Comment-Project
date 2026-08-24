package conf

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Bootstrap struct {
	Server    Server    `yaml:"server"`
	Registry  Registry  `yaml:"registry"`
	Client    Client    `yaml:"client"`
	Auth      Auth      `yaml:"auth"`
	RateLimit RateLimit `yaml:"rate_limit"`
}

type Server struct {
	HTTP HTTP `yaml:"http"`
}

type HTTP struct {
	Addr    string        `yaml:"addr"`
	Timeout time.Duration `yaml:"timeout"`
}

type Client struct {
	CommentService CommentService `yaml:"comment_service"`
}

type CommentService struct {
	ServiceName string        `yaml:"service_name"`
	Endpoint    string        `yaml:"endpoint"`
	Timeout     time.Duration `yaml:"timeout"`
	TLS         TLS           `yaml:"tls"`
}

type TLS struct {
	Enabled    bool   `yaml:"enabled"`
	CAFile     string `yaml:"ca_file"`
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	ServerName string `yaml:"server_name"`
}

type Registry struct {
	Consul Consul `yaml:"consul"`
}

type Consul struct {
	Address string `yaml:"address"`
	Scheme  string `yaml:"scheme"`
}

type Auth struct {
	Role          string `yaml:"role"`
	SigningSecret string `yaml:"signing_secret"`
}

type RateLimit struct {
	Enabled bool          `yaml:"enabled"`
	Redis   Redis         `yaml:"redis"`
	User    RateLimitRule `yaml:"user"`
	IP      RateLimitRule `yaml:"ip"`
	Device  RateLimitRule `yaml:"device"`
	API     RateLimitRule `yaml:"api"`
}

type Redis struct {
	Addr         string        `yaml:"addr"`
	Password     string        `yaml:"password"`
	DB           int           `yaml:"db"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

type RateLimitRule struct {
	Rate     float64 `yaml:"rate"`
	Capacity float64 `yaml:"capacity"`
}

func Load(path string) (*Bootstrap, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Bootstrap
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	return &cfg, nil
}

func (c *Bootstrap) applyDefaults() {
	if c.Server.HTTP.Addr == "" {
		c.Server.HTTP.Addr = "0.0.0.0:8081"
	}
	if c.Server.HTTP.Timeout <= 0 {
		c.Server.HTTP.Timeout = 3 * time.Second
	}
	if c.Registry.Consul.Address == "" {
		c.Registry.Consul.Address = "127.0.0.1:8500"
	}
	if c.Registry.Consul.Scheme == "" {
		c.Registry.Consul.Scheme = "http"
	}
	if c.Client.CommentService.ServiceName == "" && c.Client.CommentService.Endpoint == "" {
		c.Client.CommentService.ServiceName = "comment-service"
	}
	if c.Client.CommentService.Timeout <= 0 {
		c.Client.CommentService.Timeout = 2 * time.Second
	}
	if c.Client.CommentService.TLS.Enabled && c.Client.CommentService.TLS.ServerName == "" {
		c.Client.CommentService.TLS.ServerName = c.Client.CommentService.ServiceName
	}
	if c.Auth.Role == "" {
		c.Auth.Role = "student"
	}
	if c.Auth.SigningSecret == "" {
		c.Auth.SigningSecret = "comment-student-dev-secret"
	}
	if c.RateLimit.Redis.Addr == "" {
		c.RateLimit.Redis.Addr = "127.0.0.1:6379"
	}
	if c.RateLimit.Redis.DialTimeout <= 0 {
		c.RateLimit.Redis.DialTimeout = time.Second
	}
	if c.RateLimit.Redis.ReadTimeout <= 0 {
		c.RateLimit.Redis.ReadTimeout = time.Second
	}
	if c.RateLimit.Redis.WriteTimeout <= 0 {
		c.RateLimit.Redis.WriteTimeout = time.Second
	}
	applyRateLimitRuleDefault(&c.RateLimit.User, 2, 5)
	applyRateLimitRuleDefault(&c.RateLimit.IP, 20, 50)
	applyRateLimitRuleDefault(&c.RateLimit.Device, 3, 10)
	applyRateLimitRuleDefault(&c.RateLimit.API, 1000, 2000)
}

func applyRateLimitRuleDefault(rule *RateLimitRule, rate, capacity float64) {
	if rule.Rate <= 0 {
		rule.Rate = rate
	}
	if rule.Capacity <= 0 {
		rule.Capacity = capacity
	}
}
