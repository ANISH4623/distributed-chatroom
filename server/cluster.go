package main

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	pb "distributed-chatroom/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NodeInfo contains information about a cluster node
type NodeInfo struct {
	ID      string
	Address string
	Client  pb.ClusterServiceClient
	Conn    *grpc.ClientConn
}

// Cluster manages cluster coordination
type Cluster struct {
	nodeID    string
	address   string
	state     *State
	nodes     map[string]*NodeInfo
	mu        sync.RWMutex
	seedNodes []string
}

// NewCluster creates a new cluster coordinator
func NewCluster(nodeID, address string, state *State, seedNodes []string) *Cluster {
	return &Cluster{
		nodeID:    nodeID,
		address:   address,
		state:     state,
		nodes:     make(map[string]*NodeInfo),
		seedNodes: seedNodes,
	}
}

// Start initializes the cluster and connects to seed nodes
func (c *Cluster) Start() {
	log.Printf("Starting cluster with node ID: %s", c.nodeID)
	
	// Connect to seed nodes
	for _, seedAddr := range c.seedNodes {
		if seedAddr != "" && seedAddr != c.address {
			go c.connectToNode(seedAddr)
		}
	}

	// Start periodic sync
	go c.periodicSync()
	
	// Start periodic health check
	go c.healthCheck()
}

// connectToNode establishes a connection to another node
func (c *Cluster) connectToNode(address string) {
	log.Printf("Attempting to connect to node at %s", address)
	
	// Maximum backoff delay in seconds
	const maxBackoffSeconds = 30
	
	// Retry with exponential backoff
	for retries := 0; retries < 10; retries++ {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to connect to %s: %v (retry %d)", address, err, retries+1)
			backoff := 1 << retries
			if backoff > maxBackoffSeconds {
				backoff = maxBackoffSeconds
			}
			time.Sleep(time.Duration(backoff) * time.Second)
			continue
		}

		client := pb.NewClusterServiceClient(conn)
		
		// Try to ping the node
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.Ping(ctx, &pb.PingRequest{NodeId: c.nodeID})
		cancel()
		
		if err != nil {
			conn.Close()
			log.Printf("Failed to ping %s: %v (retry %d)", address, err, retries+1)
			backoff := 1 << retries
			if backoff > maxBackoffSeconds {
				backoff = maxBackoffSeconds
			}
			time.Sleep(time.Duration(backoff) * time.Second)
			continue
		}

		// Connection successful
		c.mu.Lock()
		c.nodes[resp.NodeId] = &NodeInfo{
			ID:      resp.NodeId,
			Address: address,
			Client:  client,
			Conn:    conn,
		}
		c.mu.Unlock()
		
		log.Printf("Connected to node %s at %s", resp.NodeId, address)

		// Announce ourselves to the cluster
		c.announceJoin(client)
		
		// Sync state from this node
		c.syncFromNode(client)
		
		return
	}
	
	log.Printf("Failed to connect to %s after 10 retries", address)
}

// announceJoin announces this node to another node
func (c *Cluster) announceJoin(client pb.ClusterServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.JoinCluster(ctx, &pb.JoinRequest{
		NodeId:  c.nodeID,
		Address: c.address,
	})
	if err != nil {
		log.Printf("Failed to announce join: %v", err)
		return
	}

	// Connect to other known nodes
	for _, nodeAddr := range resp.KnownNodes {
		if nodeAddr != c.address {
			c.mu.RLock()
			alreadyConnected := false
			for _, node := range c.nodes {
				if node.Address == nodeAddr {
					alreadyConnected = true
					break
				}
			}
			c.mu.RUnlock()
			
			if !alreadyConnected {
				go c.connectToNode(nodeAddr)
			}
		}
	}
}

// syncFromNode syncs state from another node
func (c *Cluster) syncFromNode(client pb.ClusterServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Sync(ctx, &pb.SyncRequest{
		NodeId:       c.nodeID,
		LastSyncTime: 0, // Full sync
	})
	if err != nil {
		log.Printf("Failed to sync from node: %v", err)
		return
	}

	// Merge received rooms into local state
	c.state.MergeRooms(resp.Rooms)
	log.Printf("Synced %d rooms from cluster", len(resp.Rooms))
}

// periodicSync periodically syncs state with other nodes
func (c *Cluster) periodicSync() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.RLock()
		nodes := make([]*NodeInfo, 0, len(c.nodes))
		for _, node := range c.nodes {
			nodes = append(nodes, node)
		}
		c.mu.RUnlock()

		for _, node := range nodes {
			c.syncFromNode(node.Client)
		}
	}
}

// healthCheck periodically checks node health
func (c *Cluster) healthCheck() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		nodesToRemove := []string{}
		
		for nodeID, node := range c.nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := node.Client.Ping(ctx, &pb.PingRequest{NodeId: c.nodeID})
			cancel()
			
			if err != nil {
				log.Printf("Node %s at %s is unreachable, removing from cluster", nodeID, node.Address)
				nodesToRemove = append(nodesToRemove, nodeID)
				node.Conn.Close()
			}
		}
		
		for _, nodeID := range nodesToRemove {
			delete(c.nodes, nodeID)
		}
		c.mu.Unlock()
	}
}

// BroadcastMessage sends a message to all other nodes
func (c *Cluster) BroadcastMessage(msg *pb.Message) {
	c.mu.RLock()
	nodes := make([]*NodeInfo, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	c.mu.RUnlock()

	for _, node := range nodes {
		go func(n *NodeInfo) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			
			_, err := n.Client.BroadcastMessage(ctx, &pb.MessageRequest{Message: msg})
			if err != nil {
				log.Printf("Failed to broadcast message to node %s: %v", n.ID, err)
			}
		}(node)
	}
}

// BroadcastRoom sends a new room to all other nodes
func (c *Cluster) BroadcastRoom(room *pb.Room) {
	c.mu.RLock()
	nodes := make([]*NodeInfo, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	c.mu.RUnlock()

	for _, node := range nodes {
		go func(n *NodeInfo) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			
			_, err := n.Client.BroadcastRoom(ctx, &pb.RoomRequest{Room: room})
			if err != nil {
				log.Printf("Failed to broadcast room to node %s: %v", n.ID, err)
			}
		}(node)
	}
}

// AddNode adds a node to the cluster
func (c *Cluster) AddNode(nodeID, address string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already connected
	if _, exists := c.nodes[nodeID]; exists {
		return
	}

	// Connect to the node
	go c.connectToNode(address)
}

// GetNodeAddresses returns addresses of all known nodes
func (c *Cluster) GetNodeAddresses() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	addresses := []string{c.address}
	for _, node := range c.nodes {
		addresses = append(addresses, node.Address)
	}
	return addresses
}

// GetNodeCount returns the number of connected nodes
func (c *Cluster) GetNodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.nodes)
}

// ParseSeedNodes parses a comma-separated list of seed node addresses
func ParseSeedNodes(seedNodesEnv string) []string {
	if seedNodesEnv == "" {
		return []string{}
	}
	return strings.Split(seedNodesEnv, ",")
}
