# Distributed Chatroom

A production-ready distributed chatroom server system built in Go with WebSocket client-server communication and gRPC for server-to-server clustering.

## Features

### Server Features
- **Create chat-rooms** - Users can create new rooms
- **List all existing rooms** - View all available rooms
- **Join existing chat-rooms** - Connect to rooms and receive all previous messages
- **Leave chat-rooms** - Disconnect from rooms
- **Message persistence** - All messages stored as long as room exists
- **Support 10+ concurrent clients** per server node
- **Multi-node clustering** - Servers connect and synchronize automatically
- **Fault tolerance** - System continues working if nodes fail
- **Data consistency** - Clients on different servers see the same content

### Client Features
- **Create rooms**
- **List existing rooms**
- **Join and leave rooms**
- **Send messages to rooms**
- **View message history** when joining a room
- **Real-time message display** with max 1.5 second delay
- **Multi-room support**

## Architecture

### Server Architecture
1. **WebSocket Server** - Handles client connections on port 8080
2. **gRPC Server** - Inter-server cluster communication on port 9090
3. **State Manager** - Thread-safe room/message storage with RWMutex
4. **Cluster Coordinator** - Discovers other nodes and syncs data
5. **Message Broadcaster** - Propagates messages to all clients

### Project Structure
```
distributed-chatroom/
├── server/
│   ├── main.go           # Server entry point
│   ├── websocket.go      # WebSocket client handling
│   ├── grpc_server.go    # Inter-server gRPC communication
│   ├── state.go          # Thread-safe state management
│   ├── cluster.go        # Cluster coordination
│   └── Dockerfile
├── client/
│   ├── main.go           # Client entry point
│   ├── websocket_client.go # WebSocket connection handling
│   ├── cli.go            # Command-line interface
│   └── Dockerfile
├── proto/
│   ├── chatroom.proto    # Protocol buffer definitions
│   ├── chatroom.pb.go    # Generated protobuf code
│   └── chatroom_grpc.pb.go # Generated gRPC code
├── docker-compose.yml    # 3-node cluster deployment
├── go.mod
├── go.sum
└── README.md
```

## Quick Start

### Prerequisites
- Go 1.22 or later
- Docker and Docker Compose (for cluster deployment)
- Protocol Buffers compiler (for proto regeneration)

### Running Locally

1. **Start a single server:**
```bash
go run ./server
```

2. **Start the client:**
```bash
go run ./client -server ws://localhost:8080/ws -username YourName
```

### Running with Docker Compose (3-Node Cluster)

1. **Start the cluster:**
```bash
docker-compose up --build
```

This starts 3 server nodes:
- Server 1: WebSocket on port 8081, gRPC on port 9091
- Server 2: WebSocket on port 8082, gRPC on port 9092
- Server 3: WebSocket on port 8083, gRPC on port 9093

2. **Connect clients to different servers:**
```bash
# Terminal 1 - Connect to server 1
go run ./client -server ws://localhost:8081/ws -username User1

# Terminal 2 - Connect to server 2
go run ./client -server ws://localhost:8082/ws -username User2

# Terminal 3 - Connect to server 3
go run ./client -server ws://localhost:8083/ws -username User3
```

## Client Commands

| Command | Description |
|---------|-------------|
| `/create <name>` | Create a new room |
| `/list` | List all available rooms |
| `/join <id>` | Join a room (use room ID or number from list) |
| `/leave` | Leave the current room |
| `/rooms` | Show all joined rooms |
| `/switch <id>` | Switch to a different joined room |
| `/help` | Show help message |
| `/quit` | Exit the client |

To send a message, just type and press Enter while in a room.

## Environment Variables

### Server
| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | auto-generated | Unique identifier for this node |
| `WS_PORT` | 8080 | WebSocket server port |
| `GRPC_PORT` | 9090 | gRPC server port |
| `CLUSTER_ADDR` | localhost:9090 | Address for cluster communication |
| `SEED_NODES` | (empty) | Comma-separated list of seed node addresses |

### Client
| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_URL` | ws://localhost:8080/ws | WebSocket server URL |
| `USERNAME` | (prompted) | Username for chat |

## Technical Details

### Communication Protocols
- **Client-Server**: WebSocket with JSON messages
- **Server-Server**: gRPC with Protocol Buffers

### Thread Safety
- State management uses `sync.RWMutex` for concurrent read/write access
- Client connections are managed with per-client mutexes
- Message channels use buffered channels to prevent blocking

### Cluster Synchronization
- Automatic node discovery through seed nodes
- Periodic state synchronization (every 30 seconds)
- Health checks to detect and remove failed nodes
- Eventual consistency model for message propagation

### Message Format

#### Client to Server
```json
{
  "type": "create_room|list_rooms|join_room|leave_room|send_message",
  "room_id": "room-uuid",
  "room_name": "Room Name",
  "username": "Username",
  "content": "Message content"
}
```

#### Server to Client
```json
{
  "type": "room_created|room_list|room_joined|room_left|message|error",
  "room_id": "room-uuid",
  "room_name": "Room Name",
  "rooms": [{"id": "...", "name": "..."}],
  "message": {"id": "...", "username": "...", "content": "...", "timestamp": 123},
  "messages": [...],
  "error": "Error message",
  "success": true
}
```

## Testing

### Manual Testing

1. Start the cluster with Docker Compose
2. Connect multiple clients to different servers
3. Create a room from one client
4. Verify the room appears on all servers using `/list`
5. Join the room from multiple clients
6. Send messages and verify they appear on all clients
7. Stop one server and verify others continue working

### Verifying Requirements

- **Concurrent Clients**: Connect 10+ clients to a single server
- **Message Sync**: Send messages from one client and verify delivery on all
- **Room Sync**: Create rooms on one server and list from another
- **Fault Tolerance**: Stop a server and verify others continue
- **Message History**: Join a room and verify historical messages appear

## License

MIT License