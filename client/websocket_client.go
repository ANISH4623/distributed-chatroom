package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ClientMessage represents messages from client to server
type ClientMessage struct {
	Type     string `json:"type"`
	RoomID   string `json:"room_id,omitempty"`
	RoomName string `json:"room_name,omitempty"`
	Username string `json:"username,omitempty"`
	Content  string `json:"content,omitempty"`
}

// ServerMessage represents messages from server to client
type ServerMessage struct {
	Type      string        `json:"type"`
	RoomID    string        `json:"room_id,omitempty"`
	RoomName  string        `json:"room_name,omitempty"`
	Rooms     []RoomInfo    `json:"rooms,omitempty"`
	Message   *MessageInfo  `json:"message,omitempty"`
	Messages  []MessageInfo `json:"messages,omitempty"`
	Error     string        `json:"error,omitempty"`
	Success   bool          `json:"success"`
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

// WSClient handles WebSocket connection to server
type WSClient struct {
	conn        *websocket.Conn
	serverURL   string
	username    string
	mu          sync.Mutex
	messages    chan ServerMessage
	done        chan struct{}
	connected   bool
	joinedRooms map[string]string // roomID -> roomName
}

// NewWSClient creates a new WebSocket client
func NewWSClient(serverURL, username string) *WSClient {
	return &WSClient{
		serverURL:   serverURL,
		username:    username,
		messages:    make(chan ServerMessage, 100),
		done:        make(chan struct{}),
		joinedRooms: make(map[string]string),
	}
}

// Connect establishes connection to the server
func (c *WSClient) Connect() error {
	conn, _, err := websocket.DefaultDialer.Dial(c.serverURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.conn = conn
	c.connected = true

	// Start message receiver
	go c.receiveMessages()

	return nil
}

// receiveMessages reads messages from the server
func (c *WSClient) receiveMessages() {
	defer func() {
		c.connected = false
		close(c.done)
	}()

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Connection error: %v", err)
			}
			return
		}

		var serverMsg ServerMessage
		if err := json.Unmarshal(message, &serverMsg); err != nil {
			log.Printf("Error parsing message: %v", err)
			continue
		}

		// Update joined rooms if we joined/left a room
		if serverMsg.Type == "room_joined" {
			c.mu.Lock()
			c.joinedRooms[serverMsg.RoomID] = serverMsg.RoomName
			c.mu.Unlock()
		} else if serverMsg.Type == "room_left" {
			c.mu.Lock()
			delete(c.joinedRooms, serverMsg.RoomID)
			c.mu.Unlock()
		}

		select {
		case c.messages <- serverMsg:
		default:
			// Channel full, drop message
		}
	}
}

// Close closes the connection
func (c *WSClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// IsConnected returns whether the client is connected
func (c *WSClient) IsConnected() bool {
	return c.connected
}

// Messages returns the channel for receiving messages
func (c *WSClient) Messages() <-chan ServerMessage {
	return c.messages
}

// Done returns the channel that closes when connection ends
func (c *WSClient) Done() <-chan struct{} {
	return c.done
}

// CreateRoom creates a new room
func (c *WSClient) CreateRoom(name string) error {
	return c.send(ClientMessage{
		Type:     "create_room",
		RoomName: name,
	})
}

// ListRooms lists all available rooms
func (c *WSClient) ListRooms() error {
	return c.send(ClientMessage{
		Type: "list_rooms",
	})
}

// JoinRoom joins a room by ID
func (c *WSClient) JoinRoom(roomID string) error {
	return c.send(ClientMessage{
		Type:   "join_room",
		RoomID: roomID,
	})
}

// LeaveRoom leaves a room by ID
func (c *WSClient) LeaveRoom(roomID string) error {
	return c.send(ClientMessage{
		Type:   "leave_room",
		RoomID: roomID,
	})
}

// SendMessage sends a message to a room
func (c *WSClient) SendMessage(roomID, content string) error {
	return c.send(ClientMessage{
		Type:     "send_message",
		RoomID:   roomID,
		Username: c.username,
		Content:  content,
	})
}

// send sends a message to the server
func (c *WSClient) send(msg ClientMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	return c.conn.WriteJSON(msg)
}

// GetJoinedRooms returns the list of joined rooms
func (c *WSClient) GetJoinedRooms() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	rooms := make(map[string]string)
	for k, v := range c.joinedRooms {
		rooms[k] = v
	}
	return rooms
}

// WaitForResponse waits for a response with timeout
func (c *WSClient) WaitForResponse(timeout time.Duration) (*ServerMessage, error) {
	select {
	case msg := <-c.messages:
		return &msg, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for response")
	case <-c.done:
		return nil, fmt.Errorf("connection closed")
	}
}
