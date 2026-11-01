package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:18080", "listen address")
	modeFile := flag.String("mode-file", "/tmp/cnld-mode", "file containing yes/no/portal/redirect/slow/large")
	flag.Parse()
	http.HandleFunc("/check", func(w http.ResponseWriter, r *http.Request) {
		mode, err := os.ReadFile(*modeFile)
		if err != nil {
			mode = []byte("yes")
		}
		switch string(mode) {
		case "yes":
			w.Write([]byte("authenticated"))
		case "no":
			w.Write([]byte("unauthenticated"))
		case "portal":
			http.Redirect(w, r, "http://login.example.edu/", http.StatusFound)
		case "redirect":
			http.Redirect(w, r, "http://other.example.edu/", http.StatusFound)
		case "slow":
			select {
			case <-time.After(10 * time.Second):
			case <-r.Context().Done():
			}
		case "large":
			w.Write(make([]byte, 8192))
		default:
			w.Write([]byte("unexpected"))
		}
	})
	log.Fatal(http.ListenAndServe(*listen, nil))
}
