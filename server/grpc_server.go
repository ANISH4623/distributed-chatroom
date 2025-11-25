package main

import (
	"context"
	"log"
	"net"
	"time"

	pb "distributed-chatroom/proto"

	"google.golang.org/grpc"
)

// GRPCServer handles inter-server communication
type GRPCServer struct {
	pb.UnimplementedClusterServiceServer
	state    *State
	nodeID   string
	cluster  *Cluster
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(state *State, nodeID string) *GRPCServer {
	return &GRPCServer{
		state:  state,
		nodeID: nodeID,
	}
}

// SetCluster sets the cluster reference
func (s *GRPCServer) SetCluster(cluster *Cluster) {
	s.cluster = cluster
}

// Start starts the gRPC server on the specified port
func (s *GRPCServer) Start(port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	pb.RegisterClusterServiceServer(grpcServer, s)

	log.Printf("gRPC server listening on port %s", port)
	return grpcServer.Serve(lis)
}

// Sync handles state synchronization requests
func (s *GRPCServer) Sync(ctx context.Context, req *pb.SyncRequest) (*pb.SyncResponse, error) {
	log.Printf("Received sync request from node %s", req.NodeId)
	
	rooms := s.state.GetAllRooms()
	
	return &pb.SyncResponse{
		Rooms:    rooms,
		SyncTime: time.Now().UnixNano(),
		Success:  true,
	}, nil
}

// BroadcastMessage handles incoming messages from other nodes
func (s *GRPCServer) BroadcastMessage(ctx context.Context, req *pb.MessageRequest) (*pb.MessageResponse, error) {
	msg := req.Message
	log.Printf("Received message broadcast: room=%s, from node=%s", msg.RoomId, msg.OriginNode)
	
	// Add message locally
	s.state.AddMessage(msg)
	
	return &pb.MessageResponse{Success: true}, nil
}

// BroadcastRoom handles incoming room creation from other nodes
func (s *GRPCServer) BroadcastRoom(ctx context.Context, req *pb.RoomRequest) (*pb.RoomResponse, error) {
	room := req.Room
	log.Printf("Received room broadcast: %s (%s), from node=%s", room.Name, room.Id, room.OriginNode)
	
	// Add room locally
	s.state.AddRoom(room)
	
	return &pb.RoomResponse{Success: true}, nil
}

// Ping handles health check requests
func (s *GRPCServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		NodeId: s.nodeID,
		Alive:  true,
	}, nil
}

// JoinCluster handles new node announcements
func (s *GRPCServer) JoinCluster(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	log.Printf("Node %s at %s joining cluster", req.NodeId, req.Address)
	
	var knownNodes []string
	if s.cluster != nil {
		knownNodes = s.cluster.GetNodeAddresses()
		s.cluster.AddNode(req.NodeId, req.Address)
	}
	
	return &pb.JoinResponse{
		Success:    true,
		KnownNodes: knownNodes,
	}, nil
}
