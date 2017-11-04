package main

import (
	"net/http"
	"fmt"
	"log"
)

func main() {
	log.Fatal(http.ListenAndServe(":3000", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Hello World!!! I'm here, actually.")
	})))
}
