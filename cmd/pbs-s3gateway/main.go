package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/pbs-plus/pbs-s3gateway/internal/auth"
	"github.com/pbs-plus/pbs-s3gateway/internal/crypto"
	"github.com/pbs-plus/pbs-s3gateway/internal/gateway"
	"github.com/pbs-plus/pbs-s3gateway/internal/keymapper"
	"github.com/pbs-plus/pbs-s3gateway/internal/pbs"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	listenAddr := flag.String("listen", ":8080", "listen address")
	pbsURL := flag.String("pbs-url", "", "PBS API base URL (e.g. https://pbs:8007)")
	pbsDatastore := flag.String("pbs-datastore", "", "PBS datastore name")
	pbsToken := flag.String("pbs-token", "", "PBS API token (TOKENID:SECRET)")
	insecureTLS := flag.Bool("insecure-tls", false, "skip TLS certificate verification")
	encryptKey := flag.String("encryption-key", "", "AES-256-GCM encryption key (hex-encoded, 64 chars)")
	credsFile := flag.String("credentials", "", "path to S3 credentials file (JSON: accessKeyId → secretAccessKey)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("pbs-s3gateway %s (commit: %s, built: %s)\n", version, commit, date)
		os.Exit(0)
	}

	if *pbsURL == "" {
		*pbsURL = os.Getenv("PBS_URL")
	}
	if *pbsDatastore == "" {
		*pbsDatastore = os.Getenv("PBS_DATASTORE")
	}
	if *pbsToken == "" {
		*pbsToken = os.Getenv("PBS_TOKEN")
	}
	if *encryptKey == "" {
		*encryptKey = os.Getenv("ENCRYPTION_KEY")
	}
	if *credsFile == "" {
		*credsFile = os.Getenv("CREDENTIALS_FILE")
	}

	if *pbsURL == "" || *pbsDatastore == "" {
		log.Fatal("PBS URL and datastore are required (via flags or PBS_URL, PBS_DATASTORE env vars)")
	}
	if *pbsToken == "" && *credsFile == "" {
		log.Fatal("Either --pbs-token or --credentials is required")
	}

	config := pbs.Config{
		BaseURL:   *pbsURL,
		Datastore: *pbsDatastore,
		AuthToken: *pbsToken,
	}

	enc, err := crypto.NewEncryptor(*encryptKey)
	if err != nil {
		log.Fatalf("invalid encryption key: %v", err)
	}

	var creds *auth.Store
	if *credsFile != "" {
		creds, err = auth.LoadStore(*credsFile)
		if err != nil {
			log.Fatalf("load credentials: %v", err)
		}
		log.Printf("Credentials: loaded from %s", *credsFile)
	}

	km := keymapper.NewKeyMapper()
	client := pbs.NewClient(config)
	uploader := pbs.NewUploader(config, *insecureTLS)

	handler := gateway.NewHandler(km, client, uploader, enc, creds)

	mux := http.NewServeMux()
	mux.Handle("/", handler)

	if enc.Enabled() {
		log.Printf("Encryption: AES-256-GCM enabled")
	} else {
		log.Printf("Encryption: disabled (passthrough)")
	}
	log.Printf("S3 gateway listening on %s -> PBS %s/%s", *listenAddr, *pbsURL, *pbsDatastore)
	if err := http.ListenAndServe(*listenAddr, mux); err != nil {
		log.Fatal(err)
	}
}
