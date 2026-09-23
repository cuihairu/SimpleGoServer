package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/proto"
	"github.com/cuihairu/simplegoserver/pkg/reactor"
	"github.com/cuihairu/simplegoserver/pkg/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// echoInitializer wires the demo echo service into every connection: the
// frame codec decodes bytes into frames, the protocol handler answers
// requests, subscriptions and heartbeats.
func echoInitializer(p handler.Pipeline) error {
	_ = p.AddLast(
		proto.NewFrameCodec(),
		proto.NewProtocolHandler(func(action string, data []byte) (any, error) {
			return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
		}),
	)
	return nil
}

var (
	cfgFile    string
	host       string
	port       int
	network    string
	multicore  bool
	numWorkers int
	lockThread bool
)

func runServer() {
	addr := fmt.Sprintf("%s://%s:%d", viper.GetString("server.network"), viper.GetString("server.host"), viper.GetInt("server.port"))
	options := reactor.ServerOptions{
		Multicore:  viper.GetBool("server.multicore"),
		NumWorkers: viper.GetInt("server.numWorkers"),
		Listener:   addr,
		LockThread: viper.GetBool("server.lockThread"),
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
	Short: "A brief description of your application",
	Long:  `A longer description that spans multiple lines and likely contains`,
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

func main() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "./config.yml", "config file")
	rootCmd.PersistentFlags().StringVar(&host, "host", "localhost", "server host")
	rootCmd.PersistentFlags().IntVar(&port, "port", 8080, "server port")
	rootCmd.PersistentFlags().StringVar(&network, "network", "tcp", "server network")
	rootCmd.PersistentFlags().BoolVar(&multicore, "multicore", true, "multicore")
	rootCmd.PersistentFlags().IntVar(&numWorkers, "workers", 10, "number of workers")
	rootCmd.PersistentFlags().BoolVar(&lockThread, "lockThread", true, "lock thread")

	// bind
	err := viper.BindPFlag("server.host", rootCmd.PersistentFlags().Lookup("host"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "bind config error: %s\n", err)
		return
	}
	err = viper.BindPFlag("server.port", rootCmd.PersistentFlags().Lookup("port"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "bind config error: %s\n", err)
		return
	}
	err = viper.BindPFlag("server.network", rootCmd.PersistentFlags().Lookup("network"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "bind config error: %s\n", err)
		return
	}
	err = viper.BindPFlag("server.multicore", rootCmd.PersistentFlags().Lookup("multicore"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "bind config error: %s\n", err)
		return
	}
	err = viper.BindPFlag("server.numWorkers", rootCmd.PersistentFlags().Lookup("workers"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "bind config error: %s\n", err)
		return
	}
	err = viper.BindPFlag("server.lockThread", rootCmd.PersistentFlags().Lookup("lockThread"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "bind config error: %s\n", err)
		return
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
