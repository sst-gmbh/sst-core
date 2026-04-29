// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// The main function of this file is to start the Remote Repository server.
// This file should be run by below command, parameters can be modified by specified application.
// Example:
//
//	go run examples/helloworldreporemote/main.go --verbose --dir=./examples/helloworldreporemote/testsstrepo/
package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/semanticstep/sst-core/defaultderive"
	"github.com/semanticstep/sst-core/sst"
	_ "github.com/semanticstep/sst-core/vocabularies/dict"
	flag "github.com/spf13/pflag"
)

func main() {
	// parse command line arguments to app struct
	app := newApplication(os.Exit, os.Stdout)
	err := app.options.Parse(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			app.osExit(1)
		}
		return
	}

	indexBleveDir := "index.bleve"
	if app.rebuildIndex {
		blevePath := filepath.Join(app.repoDir, indexBleveDir)
		if err := os.RemoveAll(blevePath); err != nil {
			log.Fatalf("failed to remove bleve index directory: %v", err)
		} else {
			log.Printf("bleve index directory: %s is removed or not existed before", blevePath)
		}
	}

	// set derive info to app struct
	app.DeriveInfo = defaultderive.DeriveInfo()

	// Listen announces on the local network address by app.bindAddress.
	lis, err := net.Listen("tcp", app.bindAddress)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// load server TLS certificate if provided
	var serverCert *tls.Certificate
	if app.credCertFile != "" || app.credKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(app.credCertFile, app.credKeyFile)
		if err != nil {
			log.Fatalf("failed to load TLS certificate: %v", err)
		}
		serverCert = &cert
	}

	// construct SST server configuration
	config := sst.RepositoryServerConfig{
		RepoDir:    app.repoDir,
		Issuer:     app.oidcIssuer, // keycloak OIDC issuer URL
		ClientID:   app.clientID,
		ServerCert: serverCert,
		Verbose:    app.verbose,
		DeriveInfo: app.DeriveInfo,
	}
	// create SST RemoteRepository server
	s, err := sst.NewServer(&config)
	if err != nil {
		return
	}

	// if verbose, print server configuration
	if app.verbose {
		var e []string
		configJSON, err := json.MarshalIndent(struct {
			FilePath, BindAddress  string
			ExtraRepositoryOptions []string `json:",omitempty"`
			*sst.RepositoryServerConfig
		}{FilePath: app.repoDir, BindAddress: app.bindAddress, ExtraRepositoryOptions: e, RepositoryServerConfig: &config}, "  ", " ")
		if err != nil {
			return
		}
		log.Printf("starting server with config:\n  %s", configJSON)
	}

	// handle interrupt signal for graceful stop
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		if err := s.GracefulStopAndClose(); err != nil {
			log.Fatalf("failed to stop: %v", err)
		}
	}()

	// start serving SST repository service
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	if app.verbose {
		log.Println("terminating...")
	}
}

type remoteRepositoryApplication struct {
	options        *flag.FlagSet
	verbose        bool
	bindAddress    string
	repoDir        string
	oidcIssuer     string
	clientID       string
	credCertFile   string
	credKeyFile    string
	fullTextSearch bool
	rebuildIndex   bool
	osExit         func(code int)
	osStdout       io.Writer
	DeriveInfo     *sst.SSTDeriveInfo
}

func newApplication(
	osExit func(code int), osStdout io.Writer,
) *remoteRepositoryApplication {
	const appName = "repository"
	options := flag.NewFlagSet(appName, flag.ContinueOnError)
	var app remoteRepositoryApplication
	options.Usage = func() {
		fmt.Fprintf(os.Stderr, "%[1]s is SST Repository server\n"+
			"Usage of %[1]s:\n"+
			"Options:\n", appName)
		options.PrintDefaults()
	}
	app = remoteRepositoryApplication{
		options:  options,
		osExit:   osExit,
		osStdout: osStdout,
	}
	options.BoolVarP(&app.verbose, "verbose", "v", false, "verbose output")
	options.StringVar(&app.bindAddress, "bind", "localhost:5581", "bind address")
	options.StringVarP(&app.repoDir, "dir", "d", "", "repository folder")
	options.StringVar(&app.oidcIssuer, "issuer", "", "OIDC issuer")
	options.StringVar(&app.clientID, "client-id", "", "OIDC client ID")
	options.StringVar(&app.credCertFile, "cert", "", "TLS credential certificate file")
	options.StringVar(&app.credKeyFile, "key", "", "TLS credential key file")
	options.BoolVarP(&app.fullTextSearch, "full-text-search", "f", true, "support full text search")
	options.BoolVarP(&app.rebuildIndex, "rebuild-index", "r", false, "rebuild bleve index")

	return &app
}
