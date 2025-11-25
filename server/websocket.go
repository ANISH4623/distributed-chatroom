package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	pb "distributed-chatroom/proto"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// ClientMessage represents messages from client to server
type ClientMessage struct {
	Type     string `json:"type"`     // create_room, list_rooms, join_room, leave_room, send_message
	RoomID   string `json:"room_id"`  // For join, leave, send
	RoomName string `json:"room_name"` // For create_room
	Username string `json:"username"` // For send_message
	Content  string `json:"content"`  // For send_message
}

// ServerMessage represents messages from server to client
type ServerMessage struct {
	Type      string      `json:"type"`      // room_created, room_list, room_joined, room_left, message, error, history
	RoomID    string      `json:"room_id,omitempty"`
	RoomName  string      `json:"room_name,omitempty"`
	Rooms     []RoomInfo  `json:"rooms,omitempty"`
	Message   *MessageInfo `json:"message,omitempty"`
	Messages  []MessageInfo `json:"messages,omitempty"`
	Error     string      `json:"error,omitempty"`
	Success   bool        `json:"success"`
}

// RoomInfo is a simplified room info for clients
type RoomInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// MessageInfo is a simplified message info for clients
type MessageInfo struct {
	ID        string `json:"id"`
	Username  string `json:"username"`
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp"`
}

// Client represents a connected WebSocket client
type Client struct {
	ID            string
	Conn          *websocket.Conn
	Server        *WSServer
	mu            sync.Mutex
	joinedRooms   map[string]bool
	stopChans     map[string]chan struct{}
}

// WSServer handles WebSocket connections
type WSServer struct {
	state    *State
	cluster  *Cluster
	nodeID   string
	clients  map[string]*Client
	mu       sync.RWMutex
	upgrader websocket.Upgrader
}

// NewWSServer creates a new WebSocket server
func NewWSServer(state *State, cluster *Cluster, nodeID string) *WSServer {
	return &WSServer{
		state:   state,
		cluster: cluster,
		nodeID:  nodeID,
		clients: make(map[string]*Client),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for demo purposes
			},
		},
	}
}

// HandleConnection handles a new WebSocket connection
func (ws *WSServer) HandleConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := ws.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	client := &Client{
		ID:          uuid.New().String(),
		Conn:        conn,
		Server:      ws,
		joinedRooms: make(map[string]bool),
		stopChans:   make(map[string]chan struct{}),
	}

	ws.mu.Lock()
	ws.clients[client.ID] = client
	ws.mu.Unlock()

	log.Printf("Client %s connected", client.ID)

	defer func() {
		ws.mu.Lock()
		delete(ws.clients, client.ID)
		ws.mu.Unlock()
		
		// Unsubscribe from all rooms
		for roomID := range client.joinedRooms {
			if stopChan, exists := client.stopChans[roomID]; exists {
				close(stopChan)
			}
			ws.state.Unsubscribe(roomID, client.ID)
		}
		
		conn.Close()
		log.Printf("Client %s disconnected", client.ID)
	}()

	// Handle incoming messages
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		var clientMsg ClientMessage
		if err := json.Unmarshal(message, &clientMsg); err != nil {
			client.sendError("Invalid message format")
			continue
		}

		client.handleMessage(&clientMsg)
	}
}

// handleMessage processes client messages
func (c *Client) handleMessage(msg *ClientMessage) {
	switch msg.Type {
	case "create_room":
		c.handleCreateRoom(msg)
	case "list_rooms":
		c.handleListRooms()
	case "join_room":
		c.handleJoinRoom(msg)
	case "leave_room":
		c.handleLeaveRoom(msg)
	case "send_message":
		c.handleSendMessage(msg)
	default:
		c.sendError("Unknown message type: " + msg.Type)
	}
}

// handleCreateRoom creates a new room
func (c *Client) handleCreateRoom(msg *ClientMessage) {
	if msg.RoomName == "" {
		c.sendError("Room name is required")
		return
	}

	// Check if room already exists
	if existingRoom := c.Server.state.GetRoomByName(msg.RoomName); existingRoom != nil {
		c.sendError("Room already exists")
		return
	}

	room := c.Server.state.CreateRoom(msg.RoomName, c.Server.nodeID)
	
	// Broadcast to cluster
	c.Server.cluster.BroadcastRoom(&pb.Room{
		Id:         room.Id,
		Name:       room.Name,
		CreatedAt:  room.CreatedAt,
		OriginNode: room.OriginNode,
		Messages:   []*pb.Message{},
	})

	c.send(ServerMessage{
		Type:     "room_created",
		RoomID:   room.Id,
		RoomName: room.Name,
		Success:  true,
	})
}

// handleListRooms lists all rooms
func (c *Client) handleListRooms() {
	rooms := c.Server.state.ListRooms()
	roomInfos := make([]RoomInfo, len(rooms))
	for i, room := range rooms {
		roomInfos[i] = RoomInfo{
			ID:   room.Id,
			Name: room.Name,
		}
	}

	c.send(ServerMessage{
		Type:    "room_list",
		Rooms:   roomInfos,
		Success: true,
	})
}

// handleJoinRoom joins a room
func (c *Client) handleJoinRoom(msg *ClientMessage) {
	roomID := msg.RoomID
	if roomID == "" {
		c.sendError("Room ID is required")
		return
	}

	room := c.Server.state.GetRoom(roomID)
	if room == nil {
		c.sendError("Room not found")
		return
	}

	if c.joinedRooms[roomID] {
		c.sendError("Already in this room")
		return
	}

	// Subscribe to room messages
	msgChan := c.Server.state.Subscribe(roomID, c.ID)
	if msgChan == nil {
		c.sendError("Failed to join room")
		return
	}

	c.mu.Lock()
	c.joinedRooms[roomID] = true
	stopChan := make(chan struct{})
	c.stopChans[roomID] = stopChan
	c.mu.Unlock()

	// Send message history
	messages := c.Server.state.GetMessages(roomID)
	messageInfos := make([]MessageInfo, len(messages))
	for i, m := range messages {
		messageInfos[i] = MessageInfo{
			ID:        m.Id,
			Username:  m.Username,
			Content:   m.Content,
			Timestamp: m.Timestamp,
		}
	}

	c.send(ServerMessage{
		Type:     "room_joined",
		RoomID:   roomID,
		RoomName: room.Name,
		Messages: messageInfos,
		Success:  true,
	})

	// Start goroutine to forward messages to client
	go func() {
		for {
			select {
			case pbMsg, ok := <-msgChan:
				if !ok {
					return
				}
				c.send(ServerMessage{
					Type:   "message",
					RoomID: roomID,
					Message: &MessageInfo{
						ID:        pbMsg.Id,
						Username:  pbMsg.Username,
						Content:   pbMsg.Content,
						Timestamp: pbMsg.Timestamp,
					},
					Success: true,
				})
			case <-stopChan:
				return
			}
		}
	}()
}

// handleLeaveRoom leaves a room
func (c *Client) handleLeaveRoom(msg *ClientMessage) {
	roomID := msg.RoomID
	if roomID == "" {
		c.sendError("Room ID is required")
		return
	}

	c.mu.Lock()
	if !c.joinedRooms[roomID] {
		c.mu.Unlock()
		c.sendError("Not in this room")
		return
	}

	if stopChan, exists := c.stopChans[roomID]; exists {
		close(stopChan)
		delete(c.stopChans, roomID)
	}
	delete(c.joinedRooms, roomID)
	c.mu.Unlock()

	c.Server.state.Unsubscribe(roomID, c.ID)

	c.send(ServerMessage{
		Type:    "room_left",
		RoomID:  roomID,
		Success: true,
	})
}

// handleSendMessage sends a message to a room
func (c *Client) handleSendMessage(msg *ClientMessage) {
	if msg.RoomID == "" {
		c.sendError("Room ID is required")
		return
	}
	if msg.Content == "" {
		c.sendError("Message content is required")
		return
	}

	c.mu.Lock()
	inRoom := c.joinedRooms[msg.RoomID]
	c.mu.Unlock()

	if !inRoom {
		c.sendError("Not in this room")
		return
	}

	username := msg.Username
	if username == "" {
		username = "Anonymous"
	}

	pbMsg := &pb.Message{
		Id:         uuid.New().String(),
		RoomId:     msg.RoomID,
		Username:   username,
		Content:    msg.Content,
		Timestamp:  time.Now().UnixNano(),
		OriginNode: c.Server.nodeID,
	}

	// Add message locally
	if !c.Server.state.AddMessage(pbMsg) {
		c.sendError("Failed to send message")
		return
	}

	// Broadcast to cluster
	c.Server.cluster.BroadcastMessage(pbMsg)

	c.send(ServerMessage{
		Type:    "message_sent",
		RoomID:  msg.RoomID,
		Success: true,
	})
}

// send sends a message to the client
func (c *Client) send(msg ServerMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.Conn.WriteJSON(msg); err != nil {
		log.Printf("Error sending message to client %s: %v", c.ID, err)
	}
}

// sendError sends an error message to the client
func (c *Client) sendError(errMsg string) {
	c.send(ServerMessage{
		Type:    "error",
		Error:   errMsg,
		Success: false,
	})
}
