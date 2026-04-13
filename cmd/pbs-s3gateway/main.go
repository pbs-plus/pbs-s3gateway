package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

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
		log.Println("Warning: no --pbs-token or --credentials configured; requests must carry S3 auth that maps to a PBS token")
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
	uploader := pbs.NewUploader(config, client, *insecureTLS)

	handler := gateway.NewHandler(km, client, uploader, enc, creds)

	// Log the chunk threshold for visibility
	log.Printf("Upload: chunk threshold = %d bytes (files larger than this will use .didx archives)",
		uploader.GetChunkThreshold())

	mux := http.NewServeMux()

	// Liveness probe - lightweight, always returns 200 if server is running
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Readiness probe - checks PBS connectivity
	// IMPORTANT: Configure your Kubernetes probe with:
	//   timeoutSeconds: 5  (must be > the 4s context timeout below)
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		// Use 4s timeout to ensure we respond before typical K8s probe timeout (default 1s)
		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()

		if err := client.HealthCheck(ctx); err != nil {
			log.Printf("Health check failed: %v", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("unready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})

	// Backward compatibility: /health acts as readiness probe
	// IMPORTANT: Configure your Kubernetes probe with:
	//   timeoutSeconds: 5  (must be > the 4s context timeout below)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		// Use 4s timeout to ensure we respond before typical K8s probe timeout (default 1s)
		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()

		if err := client.HealthCheck(ctx); err != nil {
			log.Printf("Health check failed: %v", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("unhealthy"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("healthy"))
	})

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
