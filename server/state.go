package main

import (
	"sync"
	"time"

	pb "distributed-chatroom/proto"

	"github.com/google/uuid"
)

// State manages the thread-safe storage of rooms and messages
type State struct {
	mu          sync.RWMutex
	rooms       map[string]*pb.Room
	subscribers map[string]map[string]chan *pb.Message // roomID -> clientID -> channel
}

// NewState creates a new state manager
func NewState() *State {
	return &State{
		rooms:       make(map[string]*pb.Room),
		subscribers: make(map[string]map[string]chan *pb.Message),
	}
}

// CreateRoom creates a new chat room
func (s *State) CreateRoom(name, originNode string) *pb.Room {
	s.mu.Lock()
	defer s.mu.Unlock()

	room := &pb.Room{
		Id:         uuid.New().String(),
		Name:       name,
		CreatedAt:  time.Now().UnixNano(),
		OriginNode: originNode,
		Messages:   []*pb.Message{},
	}
	s.rooms[room.Id] = room
	s.subscribers[room.Id] = make(map[string]chan *pb.Message)
	return room
}

// AddRoom adds an existing room (used for cluster sync)
func (s *State) AddRoom(room *pb.Room) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.rooms[room.Id]; exists {
		return false
	}
	s.rooms[room.Id] = room
	s.subscribers[room.Id] = make(map[string]chan *pb.Message)
	return true
}

// GetRoom gets a room by ID
func (s *State) GetRoom(roomID string) *pb.Room {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rooms[roomID]
}

// GetRoomByName gets a room by name
func (s *State) GetRoomByName(name string) *pb.Room {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, room := range s.rooms {
		if room.Name == name {
			return room
		}
	}
	return nil
}

// ListRooms returns all rooms
func (s *State) ListRooms() []*pb.Room {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rooms := make([]*pb.Room, 0, len(s.rooms))
	for _, room := range s.rooms {
		rooms = append(rooms, room)
	}
	return rooms
}

// AddMessage adds a message to a room and notifies subscribers
func (s *State) AddMessage(msg *pb.Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	room, exists := s.rooms[msg.RoomId]
	if !exists {
		return false
	}

	// Check for duplicate message
	for _, existingMsg := range room.Messages {
		if existingMsg.Id == msg.Id {
			return false
		}
	}

	room.Messages = append(room.Messages, msg)

	// Notify all subscribers of this room
	if subs, exists := s.subscribers[msg.RoomId]; exists {
		for _, ch := range subs {
			select {
			case ch <- msg:
			default:
				// Channel is full, skip
			}
		}
	}

	return true
}

// GetMessages gets all messages for a room
func (s *State) GetMessages(roomID string) []*pb.Message {
	s.mu.RLock()
	defer s.mu.RUnlock()

	room, exists := s.rooms[roomID]
	if !exists {
		return nil
	}
	return room.Messages
}

// Subscribe subscribes to messages in a room
func (s *State) Subscribe(roomID, clientID string) chan *pb.Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.rooms[roomID]; !exists {
		return nil
	}

	ch := make(chan *pb.Message, 100)
	if s.subscribers[roomID] == nil {
		s.subscribers[roomID] = make(map[string]chan *pb.Message)
	}
	s.subscribers[roomID][clientID] = ch
	return ch
}

// Unsubscribe removes a subscriber from a room
func (s *State) Unsubscribe(roomID, clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if subs, exists := s.subscribers[roomID]; exists {
		if ch, exists := subs[clientID]; exists {
			close(ch)
			delete(subs, clientID)
		}
	}
}

// GetAllRooms returns all rooms with their messages (for sync)
func (s *State) GetAllRooms() []*pb.Room {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rooms := make([]*pb.Room, 0, len(s.rooms))
	for _, room := range s.rooms {
		rooms = append(rooms, room)
	}
	return rooms
}

// MergeRooms merges rooms from another node
func (s *State) MergeRooms(rooms []*pb.Room) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, newRoom := range rooms {
		if existingRoom, exists := s.rooms[newRoom.Id]; exists {
			// Merge messages
			existingMsgIDs := make(map[string]bool)
			for _, msg := range existingRoom.Messages {
				existingMsgIDs[msg.Id] = true
			}
			for _, msg := range newRoom.Messages {
				if !existingMsgIDs[msg.Id] {
					existingRoom.Messages = append(existingRoom.Messages, msg)
				}
			}
		} else {
			// Add new room
			s.rooms[newRoom.Id] = newRoom
			s.subscribers[newRoom.Id] = make(map[string]chan *pb.Message)
		}
	}
}
