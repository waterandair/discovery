package conf

import (
	"flag"
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/bilibili/kratos/pkg/log"
	http "github.com/bilibili/kratos/pkg/net/http/blademaster"
)

var (
	confPath      string
	schedulerPath string
	region        string // 大区，通常设置为国家地区， 如 china
	zone          string // 机房地区，通常设置为机房所在的城市，如 bj， sh， 分别代表北京，上海机房
	deployEnv     string
	hostname      string
	// Conf conf
	Conf = &Config{}
)

func init() {
	var err error
	if hostname, err = os.Hostname(); err != nil || hostname == "" {
		hostname = os.Getenv("HOSTNAME")
	}
	flag.StringVar(&confPath, "conf", "discovery-example.toml", "config path")
	flag.StringVar(&hostname, "hostname", hostname, "machine hostname")
	flag.StringVar(&schedulerPath, "scheduler", "scheduler.json", "scheduler info")
}

// Config config.
type Config struct {
	Nodes         []string
	Zones         map[string][]string  // change map[string]Nodes
	HTTPServer    *http.ServerConfig
	HTTPClient    *http.ClientConfig
	Env           *Env
	Log           *log.Config
	Scheduler     []byte
	EnableProtect bool
}

// Fix fix env config.
func (c *Config) Fix() (err error) {
	if c.Env == nil {
		c.Env = new(Env)
	}
	if c.Env.Region == "" {
		c.Env.Region = region
	}
	if c.Env.Zone == "" {
		c.Env.Zone = zone
	}
	if c.Env.Host == "" {
		c.Env.Host = hostname
	}
	if c.Env.DeployEnv == "" {
		c.Env.DeployEnv = deployEnv
	}
	return
}

// Env is disocvery env.
type Env struct {
	Region    string
	Zone      string
	Host      string
	DeployEnv string
}

// Init init conf
func Init() (err error) {
	if _, err = toml.DecodeFile(confPath, &Conf); err != nil {
		return
	}
	if schedulerPath != "" {
		Conf.Scheduler, _ = ioutil.ReadFile(schedulerPath)
	}
	return Conf.Fix()
}
