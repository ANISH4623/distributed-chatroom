package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"
)

// CLI handles the command-line interface
type CLI struct {
	client      *WSClient
	reader      *bufio.Reader
	currentRoom string
	rooms       []RoomInfo
}

// NewCLI creates a new CLI instance
func NewCLI(client *WSClient) *CLI {
	return &CLI{
		client: client,
		reader: bufio.NewReader(os.Stdin),
	}
}

// Run starts the CLI
func (c *CLI) Run() {
	fmt.Println("==================================")
	fmt.Println("  Distributed Chatroom Client")
	fmt.Println("==================================")
	fmt.Println()

	// Start message handler
	go c.handleMessages()

	// Main command loop
	c.commandLoop()
}

// handleMessages handles incoming messages from the server
func (c *CLI) handleMessages() {
	for {
		select {
		case msg := <-c.client.Messages():
			c.handleServerMessage(msg)
		case <-c.client.Done():
			fmt.Println("\n[Disconnected from server]")
			os.Exit(1)
		}
	}
}

// handleServerMessage processes a message from the server
func (c *CLI) handleServerMessage(msg ServerMessage) {
	switch msg.Type {
	case "room_created":
		fmt.Printf("\n[Room created: %s (%s)]\n", msg.RoomName, msg.RoomID)
		fmt.Print("> ")
	case "room_list":
		fmt.Println("\n[Available Rooms]")
		if len(msg.Rooms) == 0 {
			fmt.Println("  No rooms available. Create one with: /create <name>")
		} else {
			c.rooms = msg.Rooms
			for i, room := range msg.Rooms {
				fmt.Printf("  %d. %s (%s)\n", i+1, room.Name, room.ID)
			}
		}
		fmt.Print("> ")
	case "room_joined":
		c.currentRoom = msg.RoomID
		fmt.Printf("\n[Joined room: %s]\n", msg.RoomName)
		if len(msg.Messages) > 0 {
			fmt.Println("[Message History]")
			for _, m := range msg.Messages {
				timestamp := time.Unix(0, m.Timestamp).Format("15:04:05")
				fmt.Printf("  [%s] %s: %s\n", timestamp, m.Username, m.Content)
			}
			fmt.Println("[End of History]")
		}
		fmt.Print("> ")
	case "room_left":
		if c.currentRoom == msg.RoomID {
			c.currentRoom = ""
		}
		fmt.Printf("\n[Left room: %s]\n", msg.RoomID)
		fmt.Print("> ")
	case "message":
		if msg.Message != nil {
			timestamp := time.Unix(0, msg.Message.Timestamp).Format("15:04:05")
			fmt.Printf("\n[%s] %s: %s\n", timestamp, msg.Message.Username, msg.Message.Content)
			fmt.Print("> ")
		}
	case "message_sent":
		// Silent acknowledgment
	case "error":
		fmt.Printf("\n[Error: %s]\n", msg.Error)
		fmt.Print("> ")
	}
}

// commandLoop handles user input
func (c *CLI) commandLoop() {
	c.printHelp()
	fmt.Print("> ")

	for {
		input, err := c.reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			fmt.Print("> ")
			continue
		}

		c.processCommand(input)
	}
}

// processCommand processes a user command
func (c *CLI) processCommand(input string) {
	// Check if it's a command
	if strings.HasPrefix(input, "/") {
		parts := strings.SplitN(input, " ", 2)
		cmd := parts[0]
		args := ""
		if len(parts) > 1 {
			args = parts[1]
		}

		switch cmd {
		case "/create":
			c.createRoom(args)
		case "/list":
			c.listRooms()
		case "/join":
			c.joinRoom(args)
		case "/leave":
			c.leaveRoom()
		case "/rooms":
			c.showJoinedRooms()
		case "/switch":
			c.switchRoom(args)
		case "/help":
			c.printHelp()
			fmt.Print("> ")
		case "/quit", "/exit":
			fmt.Println("Goodbye!")
			os.Exit(0)
		default:
			fmt.Println("Unknown command. Type /help for available commands.")
			fmt.Print("> ")
		}
	} else {
		// Send message to current room
		c.sendMessage(input)
	}
}

// createRoom creates a new room
func (c *CLI) createRoom(name string) {
	if name == "" {
		fmt.Println("Usage: /create <room_name>")
		fmt.Print("> ")
		return
	}

	if err := c.client.CreateRoom(name); err != nil {
		fmt.Printf("Error creating room: %v\n", err)
		fmt.Print("> ")
	}
}

// listRooms lists all available rooms
func (c *CLI) listRooms() {
	if err := c.client.ListRooms(); err != nil {
		fmt.Printf("Error listing rooms: %v\n", err)
		fmt.Print("> ")
	}
}

// joinRoom joins a room
func (c *CLI) joinRoom(roomRef string) {
	if roomRef == "" {
		fmt.Println("Usage: /join <room_id or number>")
		fmt.Print("> ")
		return
	}

	// Check if it's a number (room index from list)
	var roomID string
	var idx int
	if _, err := fmt.Sscanf(roomRef, "%d", &idx); err == nil && idx > 0 && idx <= len(c.rooms) {
		roomID = c.rooms[idx-1].ID
	} else {
		roomID = roomRef
	}

	if err := c.client.JoinRoom(roomID); err != nil {
		fmt.Printf("Error joining room: %v\n", err)
		fmt.Print("> ")
	}
}

// leaveRoom leaves the current room
func (c *CLI) leaveRoom() {
	if c.currentRoom == "" {
		fmt.Println("Not in any room")
		fmt.Print("> ")
		return
	}

	if err := c.client.LeaveRoom(c.currentRoom); err != nil {
		fmt.Printf("Error leaving room: %v\n", err)
		fmt.Print("> ")
	}
}

// showJoinedRooms shows all joined rooms
func (c *CLI) showJoinedRooms() {
	rooms := c.client.GetJoinedRooms()
	if len(rooms) == 0 {
		fmt.Println("Not in any rooms")
	} else {
		fmt.Println("[Joined Rooms]")
		for id, name := range rooms {
			current := ""
			if id == c.currentRoom {
				current = " (current)"
			}
			fmt.Printf("  %s (%s)%s\n", name, id, current)
		}
	}
	fmt.Print("> ")
}

// switchRoom switches to a different joined room
func (c *CLI) switchRoom(roomRef string) {
	if roomRef == "" {
		fmt.Println("Usage: /switch <room_id>")
		fmt.Print("> ")
		return
	}

	rooms := c.client.GetJoinedRooms()
	
	// Check if it's a valid room we've joined
	for id, name := range rooms {
		if id == roomRef || name == roomRef {
			c.currentRoom = id
			fmt.Printf("[Switched to room: %s]\n", name)
			fmt.Print("> ")
			return
		}
	}

	fmt.Println("Room not found or not joined. Use /rooms to see joined rooms.")
	fmt.Print("> ")
}

// sendMessage sends a message to the current room
func (c *CLI) sendMessage(content string) {
	if c.currentRoom == "" {
		fmt.Println("Not in any room. Use /join <room_id> to join a room first.")
		fmt.Print("> ")
		return
	}

	if err := c.client.SendMessage(c.currentRoom, content); err != nil {
		fmt.Printf("Error sending message: %v\n", err)
		fmt.Print("> ")
	} else {
		fmt.Print("> ")
	}
}

// printHelp prints available commands
func (c *CLI) printHelp() {
	fmt.Println(`
Available Commands:
  /create <name>  - Create a new room
  /list           - List all available rooms
  /join <id>      - Join a room (use room ID or number from list)
  /leave          - Leave the current room
  /rooms          - Show all joined rooms
  /switch <id>    - Switch to a different joined room
  /help           - Show this help message
  /quit           - Exit the client

To send a message, just type and press Enter (must be in a room)
`)
}
