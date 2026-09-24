package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/proto"
	"github.com/cuihairu/simplegoserver/pkg/reactor"
	"github.com/cuihairu/simplegoserver/pkg/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// One shared protocol handler for every connection: the subscription table
// must be process-wide, or clients subscribed to the same topic on
// different connections would never see each other's publishes.
var demoProtocol = proto.NewProtocolHandler(func(action string, data []byte) (any, error) {
	return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
})

// echoInitializer wires the frame codec and the shared protocol handler
// into every accepted connection.
func echoInitializer(p handler.Pipeline) error {
	_ = p.AddLast(
		proto.NewFrameCodec(),
		demoProtocol,
	)
	return nil
}

var (
	cfgFile     string
	host        string
	port        int
	network     string
	multicore   bool
	numWorkers  int
	lockThread  bool
	idleTime    time.Duration
	strictHello bool
)

func runServer() {
	demoProtocol.RequireHello(viper.GetBool("server.strictHello"))
	addr := fmt.Sprintf("%s://%s:%d", viper.GetString("server.network"), viper.GetString("server.host"), viper.GetInt("server.port"))
	options := &reactor.ServerOptions{
		Multicore:   viper.GetBool("server.multicore"),
		NumWorkers:  viper.GetInt("server.numWorkers"),
		Listener:    addr,
		LockThread:  viper.GetBool("server.lockThread"),
		IdleTimeout: viper.GetDuration("server.idleTimeout"),
	}
	newReactor, err := reactor.NewReactor(options, nil, nil, echoInitializer, nil)
	if err != nil {
		panic(err)
	}
	go func() {
		newReactor.Run()
	}()

	// block on signals; SIGINT/SIGTERM trigger the staged graceful shutdown
	graceful := utils.NewGraceful(func(signal os.Signal) {
		newReactor.ShutdownGracefully()
	}, nil)
	graceful.Wait()
}

var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "SimpleGoServer: reactor-based TCP server with echo, pub/sub and streaming",
	Long: `SimpleGoServer serves the custom frame protocol (10-byte header + JSON
payload) over TCP: request/response, publish/subscribe and streamed large
payloads, with heartbeat reaping, graceful shutdown and pluggable balancers.

Configuration comes from --config (YAML, see config.example.yml); any key
can be overridden by the matching command-line flag.`,
	Run: func(cmd *cobra.Command, args []string) {
		runServer()
	},
}

func initConfig() {
	if cfgFile == "" {
		viper.AddConfigPath(".")
		viper.SetConfigName("config")
	} else {
		viper.SetConfigFile(cfgFile)
	}
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

// osExit is a variable so tests can run main() without terminating the
// test binary.
var osExit = os.Exit

// Flags are registered and bound exactly once per process (init), so main
// stays a thin, repeatable shell that tests can drive more than once.
func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "./config.yml", "config file")
	rootCmd.PersistentFlags().StringVar(&host, "host", "localhost", "server host")
	rootCmd.PersistentFlags().IntVar(&port, "port", 8080, "server port")
	rootCmd.PersistentFlags().StringVar(&network, "network", "tcp", "server network")
	rootCmd.PersistentFlags().BoolVar(&multicore, "multicore", true, "multicore")
	rootCmd.PersistentFlags().IntVar(&numWorkers, "workers", 10, "number of workers")
	rootCmd.PersistentFlags().BoolVar(&lockThread, "lockThread", true, "lock thread")
	rootCmd.PersistentFlags().DurationVar(&idleTime, "idleTimeout", 0, "close connections silent for this long (e.g. 90s; 0 disables)")
	rootCmd.PersistentFlags().BoolVar(&strictHello, "strictHello", false, "require HELLO as the first frame on every connection")
	bindFlags()
}

// bindFlags wires the registered flags into viper. Every flag is registered
// a few lines above (or in init, before this ever runs), so the lookup can
// never hand viper a nil flag and BindPFlag cannot fail — there is
// deliberately no error path.
func bindFlags() {
	_ = viper.BindPFlag("server.host", rootCmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("server.port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("server.network", rootCmd.PersistentFlags().Lookup("network"))
	_ = viper.BindPFlag("server.multicore", rootCmd.PersistentFlags().Lookup("multicore"))
	_ = viper.BindPFlag("server.numWorkers", rootCmd.PersistentFlags().Lookup("workers"))
	_ = viper.BindPFlag("server.lockThread", rootCmd.PersistentFlags().Lookup("lockThread"))
	_ = viper.BindPFlag("server.idleTimeout", rootCmd.PersistentFlags().Lookup("idleTimeout"))
	_ = viper.BindPFlag("server.strictHello", rootCmd.PersistentFlags().Lookup("strictHello"))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		osExit(1)
	}
}
