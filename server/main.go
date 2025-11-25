package main

import (
	"log"
	"net/http"
	"os"

	"github.com/google/uuid"
)

func main() {
	// Configuration from environment variables
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = uuid.New().String()[:8]
	}

	wsPort := os.Getenv("WS_PORT")
	if wsPort == "" {
		wsPort = "8080"
	}

	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "9090"
	}

	// Address for cluster communication
	clusterAddr := os.Getenv("CLUSTER_ADDR")
	if clusterAddr == "" {
		clusterAddr = "localhost:" + grpcPort
	}

	// Seed nodes for cluster discovery
	seedNodesEnv := os.Getenv("SEED_NODES")
	seedNodes := ParseSeedNodes(seedNodesEnv)

	log.Printf("Starting server with Node ID: %s", nodeID)
	log.Printf("WebSocket port: %s, gRPC port: %s", wsPort, grpcPort)
	log.Printf("Cluster address: %s", clusterAddr)
	log.Printf("Seed nodes: %v", seedNodes)

	// Initialize state
	state := NewState()

	// Initialize gRPC server
	grpcServer := NewGRPCServer(state, nodeID)

	// Initialize cluster
	cluster := NewCluster(nodeID, clusterAddr, state, seedNodes)
	grpcServer.SetCluster(cluster)

	// Initialize WebSocket server
	wsServer := NewWSServer(state, cluster, nodeID)

	// Start gRPC server in goroutine
	go func() {
		if err := grpcServer.Start(grpcPort); err != nil {
			log.Fatalf("Failed to start gRPC server: %v", err)
		}
	}()

	// Start cluster coordination
	go cluster.Start()

	// Setup HTTP routes
	http.HandleFunc("/ws", wsServer.HandleConnection)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"running","node_id":"` + nodeID + `","connected_nodes":` + 
			string(rune('0'+cluster.GetNodeCount())) + `}`))
	})

	// Start WebSocket server
	log.Printf("WebSocket server listening on port %s", wsPort)
	if err := http.ListenAndServe(":"+wsPort, nil); err != nil {
		log.Fatalf("Failed to start WebSocket server: %v", err)
	}
}
