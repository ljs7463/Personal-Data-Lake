package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	http.HandlerFunc("/",func(w http.ResponseWriter, r * http.Request) {
		log.Println("received request")
		fmt.Fprint(w, "Hello Docker!")
	})

	log.Println("start server")
	server:= & http.Server {
		Addr: ":8080"
	}
	if err := server.ListenAndServe(); err != nil {
		log.Println(err)
	}
}