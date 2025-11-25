package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	// Parse command line flags
	serverURL := flag.String("server", "", "WebSocket server URL (e.g., ws://localhost:8080/ws)")
	username := flag.String("username", "", "Username for chat")
	flag.Parse()

	// Get server URL from flag or environment
	if *serverURL == "" {
		*serverURL = os.Getenv("SERVER_URL")
	}
	if *serverURL == "" {
		*serverURL = "ws://localhost:8080/ws"
	}

	// Get username from flag or environment
	if *username == "" {
		*username = os.Getenv("USERNAME")
	}
	if *username == "" {
		fmt.Print("Enter your username: ")
		fmt.Scanln(username)
	}
	if *username == "" {
		*username = "Anonymous"
	}

	fmt.Printf("Connecting to %s as %s...\n", *serverURL, *username)

	// Create and connect client
	client := NewWSClient(*serverURL, *username)
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	fmt.Println("Connected!")

	// Start CLI
	cli := NewCLI(client)
	cli.Run()
}
