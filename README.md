# WebSocket Echo Server

A Go WebSocket echo server demonstrating various WebSocket features including message transformation, JSON command processing, rate limiting, and multi-client broadcasting.

## Features

### Core Features
- **Uppercase Echo**: Send `UPPER:text` to receive the text in uppercase
- **Reverse Echo**: Send `REVERSE:text` to receive the text reversed
- **Message Counter**: Each response includes a global message counter `[Msg #N]`
- **JSON Calculator**: Send JSON commands for math operations

### Bonus Features
- **Rate Limiting**: Max 10 messages per minute per connection
- **Command History**: Send `HISTORY` to see your last 5 commands
- **Multi-Client Broadcast**: Send `BROADCAST:message` to send to all connected clients

## Getting Started

### Prerequisites
- Go 1.21 or higher

### Installation
```bash
go mod download
```

### Running the Server
```bash
go run cmd/web/main.go
```

The server starts on http://localhost:4000

### Running with Race Detector
```bash
go run -race cmd/web/main.go
```

## Testing

1. Open http://localhost:4000/test.html in your browser
2. Click **Connect** to establish WebSocket connection
3. Use the buttons to test different features:

### Test Examples

| Button | Sends | Expected Response |
|--------|-------|-------------------|
| Send UPPER:hello world | `UPPER:hello world` | `[Msg #1] HELLO WORLD` |
| Send REVERSE:test message | `REVERSE:test message` | `[Msg #2] egassem tset` |
| JSON: add 10+5 | `{"command":"add","a":10,"b":5}` | `[Msg #3] {"result":15,"command":"add"}` |
| JSON: subtract 10-5 | `{"command":"subtract","a":10,"b":5}` | `[Msg #4] {"result":5,"command":"subtract"}` |
| JSON: multiply 10*5 | `{"command":"multiply","a":10,"b":5}` | `[Msg #5] {"result":50,"command":"multiply"}` |
| JSON: divide 10/5 | `{"command":"divide","a":10,"b":5}` | `[Msg #6] {"result":2,"command":"divide"}` |
| HISTORY | `HISTORY` | Shows last 5 commands |
| BROADCAST | `BROADCAST:Hello everyone!` | Sends to all connected clients |
| Test Rate Limit | 15 rapid messages | First 10 succeed, next 5 are rate limited |

### Multi-Client Testing
1. Open two browser tabs to http://localhost:4000/test.html
2. Connect both clients
3. Click **BROADCAST** in one tab
4. Both tabs receive the broadcast message