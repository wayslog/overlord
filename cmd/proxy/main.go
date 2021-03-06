package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof" // NOTE: use http pprof
	"os"
	"os/signal"
	"strings"
	"syscall"

	"overlord/lib/log"
	"overlord/lib/prom"
	"overlord/proxy"
)

const (
	// VERSION version
	VERSION = "1.1.0"
)

var (
	version  bool
	check    bool
	logStd   bool
	logFile  string
	logVl    int
	debug    bool
	pprof    string
	metrics  bool
	config   string
	clusters clustersFlag
)

type clustersFlag []string

func (c *clustersFlag) String() string {
	return strings.Join([]string(*c), " ")
}

func (c *clustersFlag) Set(n string) error {
	*c = append(*c, n)
	return nil
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of Overlord proxy:\n")
	flag.PrintDefaults()
}

func init() {
	flag.Usage = usage
	flag.BoolVar(&check, "t", false, "conf file check")
	flag.BoolVar(&version, "v", false, "print version.")
	flag.BoolVar(&logStd, "std", false, "log will printing into stdout.")
	flag.BoolVar(&debug, "debug", false, "debug model, will open stdout log. high priority than conf.debug.")
	flag.StringVar(&logFile, "log", "", "log will printing file {log}. high priority than conf.log.")
	flag.IntVar(&logVl, "log-vl", 0, "log verbose level. high priority than conf.log_vl.")
	flag.StringVar(&pprof, "pprof", "", "pprof listen addr. high priority than conf.pprof.")
	flag.BoolVar(&metrics, "metrics", false, "proxy support prometheus metrics and reuse pprof port.")
	flag.StringVar(&config, "conf", "", "run with the specific configuration.")
	flag.Var(&clusters, "cluster", "specify cache cluster configuration.")
}

func main() {
	flag.Parse()
	if version {
		fmt.Printf("overlord version %s\n", VERSION)
		os.Exit(0)
	}
	if check {
		parseConfig()
		os.Exit(0)
	}
	c, ccs := parseConfig()
	if initLog(c) {
		defer log.Close()
	}
	// pprof
	if c.Pprof != "" {
		go http.ListenAndServe(c.Pprof, nil)
		if c.Proxy.UseMetrics {
			prom.Init()
		} else {
			prom.On = false
		}
	}
	// new proxy
	p, err := proxy.New(c)
	if err != nil {
		panic(err)
	}
	defer p.Close()
	go p.Serve(ccs)
	// hanlde signal
	signalHandler()
}

func initLog(c *proxy.Config) bool {
	var hs []log.Handler
	if logStd || c.Debug {
		hs = append(hs, log.NewStdHandler())
	}
	if c.Log != "" {
		hs = append(hs, log.NewFileHandler(c.Log))
	}
	if len(hs) > 0 {
		log.DefaultVerboseLevel = c.LogVL
		log.Init(hs...)
		return true
	}
	return false
}

func parseConfig() (c *proxy.Config, ccs []*proxy.ClusterConfig) {
	if config != "" {
		c = &proxy.Config{}
		if err := c.LoadFromFile(config); err != nil {
			panic(err)
		}
	} else {
		c = proxy.DefaultConfig()
	}
	// high priority start
	if pprof != "" {
		c.Pprof = pprof
	}
	if metrics {
		c.Proxy.UseMetrics = metrics
	}
	if debug {
		c.Debug = debug
	}
	if logFile != "" {
		c.Log = logFile
	}
	if logVl > 0 {
		c.LogVL = logVl
	}
	// high priority end
	checks := map[string]struct{}{}
	for _, cluster := range clusters {
		cs := &proxy.ClusterConfigs{}
		if err := cs.LoadFromFile(cluster); err != nil {
			panic(err)
		}
		for _, cc := range cs.Clusters {
			if _, ok := checks[cc.Name]; ok {
				panic("the same cluster name cannot be repeated")
			}
			checks[cc.Name] = struct{}{}
		}
		ccs = append(ccs, cs.Clusters...)
	}
	return
}

func signalHandler() {
	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		log.Infof("overlord proxy version[%s] already started", VERSION)
		si := <-ch
		log.Infof("overlord proxy version[%s] signal(%s) stop the process", VERSION, si.String())
		switch si {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			log.Infof("overlord proxy version[%s] already exited", VERSION)
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}
