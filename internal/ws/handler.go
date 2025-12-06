package ws

// Filename: internal/ws/handler.go

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Heartbeat and timeout settings
const (
	writeWait  = 5 * time.Second     // max time to complete a write
	pongWait   = 30 * time.Second    // if we don't get a pong in 30s, time out
	pingPeriod = (pongWait * 9) / 10 // send pings at ~90% of pongWait (e.g., 27s)

	// Rate limiting settings
	maxMessagesPerMinute = 10
	rateLimitWindow      = time.Minute

	// Command history settings
	maxHistorySize = 5
)

// Thread-safe message counter
var messageCounter uint64

// Client management for broadcast
var (
	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.RWMutex
)

// CommandRequest represents a JSON command from the client
type CommandRequest struct {
	Command string  `json:"command"`
	A       float64 `json:"a"`
	B       float64 `json:"b"`
}

// CommandResponse represents a JSON response to the client
type CommandResponse struct {
	Result  float64 `json:"result"`
	Command string  `json:"command"`
	Error   string  `json:"error,omitempty"`
}

// processCommand handles JSON commands and returns JSON response
func processCommand(payload []byte) ([]byte, error) {
	var req CommandRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return json.Marshal(CommandResponse{Error: "invalid JSON"})
	}

	var result float64
	switch req.Command {
	case "add":
		result = req.A + req.B
	case "subtract":
		result = req.A - req.B
	case "multiply":
		result = req.A * req.B
	case "divide":
		if req.B == 0 {
			return json.Marshal(CommandResponse{Command: req.Command, Error: "division by zero"})
		}
		result = req.A / req.B
	default:
		return json.Marshal(CommandResponse{Error: "unknown command: " + req.Command})
	}

	return json.Marshal(CommandResponse{Result: result, Command: req.Command})
}

// Only allow pages served from this origin to connect
var allowedOrigins = []string{
	"http://localhost:4000",
}

func originAllowed(o string) bool {
	if o == "" {
		return false
	}
	for _, a := range allowedOrigins {
		if strings.EqualFold(o, a) {
			return true
		}
	}
	return false
}

// reverseString reverses a string, handling Unicode properly
func reverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

// checkRateLimit checks if a message is allowed based on rate limiting
// Returns true if allowed, false if rate limited
func checkRateLimit(timestamps *[]time.Time) bool {
	now := time.Now()
	cutoff := now.Add(-rateLimitWindow)

	// Remove old timestamps outside the window
	valid := []time.Time{}
	for _, t := range *timestamps {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}

	// Check if we're at the limit
	if len(valid) >= maxMessagesPerMinute {
		*timestamps = valid
		return false
	}

	// Add current timestamp and allow
	valid = append(valid, now)
	*timestamps = valid
	return true
}

// addToHistory adds a command to the history, maintaining max size
func addToHistory(history *[]string, command string) {
	*history = append(*history, command)
	if len(*history) > maxHistorySize {
		*history = (*history)[1:] // Remove oldest
	}
}

// getHistory returns the command history as a formatted string
func getHistory(history []string) string {
	if len(history) == 0 {
		return "No command history"
	}
	var result strings.Builder
	result.WriteString("Command History:\n")
	for i, cmd := range history {
		result.WriteString(fmt.Sprintf("  %d. %s\n", i+1, cmd))
	}
	return result.String()
}

// registerClient adds a client to the broadcast list
func registerClient(conn *websocket.Conn) {
	clientsMu.Lock()
	clients[conn] = true
	clientsMu.Unlock()
	log.Printf("Client registered. Total clients: %d", len(clients))
}

// unregisterClient removes a client from the broadcast list
func unregisterClient(conn *websocket.Conn) {
	clientsMu.Lock()
	delete(clients, conn)
	clientsMu.Unlock()
	log.Printf("Client unregistered. Total clients: %d", len(clients))
}

// broadcast sends a message to all connected clients
func broadcast(message string, sender *websocket.Conn) {
	clientsMu.RLock()
	defer clientsMu.RUnlock()

	for client := range clients {
		_ = client.SetWriteDeadline(time.Now().Add(writeWait))
		if err := client.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
			log.Printf("broadcast write error to client: %v", err)
		}
	}
	log.Printf("Broadcast sent to %d clients", len(clients))
}

// The upgrader object is used when we need to upgrade from HTTP to RFC 6455
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		ok := originAllowed(origin)
		if !ok {
			log.Printf("blocked cross-origin websocket: Origin=%q Path=%s", origin, r.URL.Path)
		}
		return ok
	},
	Error: func(w http.ResponseWriter, r *http.Request, status int, reason error) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
	},
}

// Attempt to upgrade from HTTP to RFC 6455
func HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Has to be an HTTP GET request
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Upgrade the connection from HTTP to RFC 6455
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade error: %v", err)
		return
	}
	defer conn.Close()

	// Register client for broadcast
	registerClient(conn)
	defer unregisterClient(conn)

	// Per-connection rate limiting timestamps
	var messageTimestamps []time.Time

	// Per-connection command history
	var commandHistory []string

	log.Printf("connection opened from %s", r.RemoteAddr)

	// Limit message size
	conn.SetReadLimit(1024 * 4)

	// PING / PONG SETUP

	// Idle timeout window starts now: must receive a pong within pongWait
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))

	// On each pong, extend the read deadline again
	conn.SetPongHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		log.Printf("pong from %s (data=%q)", r.RemoteAddr, appData)
		return nil
	})

	// Start a goroutine that sends pings every pingPeriod
	done := make(chan struct{})
	ticker := time.NewTicker(pingPeriod)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Send a ping; if this fails, the read loop will notice soon
				_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
					log.Printf("ping write error: %v", err)
					return
				}
				log.Printf("ping → %s", r.RemoteAddr)
			case <-done:
				return
			}
		}
	}()

	// Read/Echo loop
	for {
		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			// This error will be:
			//  - a timeout (no pong in time), or
			//  - a normal close, or
			//  - some other read error
			log.Printf("read error (timeout/close): %v", err)

			// Try to send a graceful close so the client can see 1000 instead of 1006
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "idle timeout"),
				time.Now().Add(writeWait),
			)

			break
		}

		// We successfully read a message; normal traffic also keeps the connection alive.
		// Note: the pong handler also updates the read deadline on pongs.

		// Echo back text messages
		if msgType == websocket.TextMessage {
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))

			// Check rate limit
			if !checkRateLimit(&messageTimestamps) {
				response := "Rate limit exceeded. Max 10 messages per minute."
				if err := conn.WriteMessage(websocket.TextMessage, []byte(response)); err != nil {
					log.Printf("write error: %v", err)
					break
				}
				continue
			}

			// Increment the counter atomically
			count := atomic.AddUint64(&messageCounter, 1)

			// Add to command history
			addToHistory(&commandHistory, string(payload))

			var response string

			// Check for HISTORY command
			if string(payload) == "HISTORY" {
				response = fmt.Sprintf("[Msg #%d]\n%s", count, getHistory(commandHistory[:len(commandHistory)-1])) // Exclude HISTORY itself
			} else if strings.HasPrefix(string(payload), "BROADCAST:") {
				// Handle broadcast
				msg := strings.TrimPrefix(string(payload), "BROADCAST:")
				broadcastMsg := fmt.Sprintf("[BROADCAST from %s] %s", r.RemoteAddr, msg)
				broadcast(broadcastMsg, conn)
				response = fmt.Sprintf("[Msg #%d] Broadcast sent: %s", count, msg)
			} else if len(payload) > 0 && payload[0] == '{' {
				// Check if payload is JSON (starts with '{')
				jsonResponse, err := processCommand(payload)
				if err != nil {
					log.Printf("JSON processing error: %v", err)
					response = fmt.Sprintf("[Msg #%d] {\"error\": \"processing error\"}", count)
				} else {
					response = fmt.Sprintf("[Msg #%d] %s", count, string(jsonResponse))
				}
			} else if strings.HasPrefix(string(payload), "UPPER:") {
				// Extract the rest and convert to uppercase
				rest := strings.TrimPrefix(string(payload), "UPPER:")
				response = fmt.Sprintf("[Msg #%d] %s", count, strings.ToUpper(rest))
			} else if strings.HasPrefix(string(payload), "REVERSE:") {
				// Extract the rest and reverse it
				rest := strings.TrimPrefix(string(payload), "REVERSE:")
				response = fmt.Sprintf("[Msg #%d] %s", count, reverseString(rest))
			} else {
				// Regular echo
				response = fmt.Sprintf("[Msg #%d] %s", count, string(payload))
			}

			if err := conn.WriteMessage(websocket.TextMessage, []byte(response)); err != nil {
				log.Printf("write error: %v", err)
				break
			}
		}
	}

	// Stop the ping goroutine
	close(done)

	log.Printf("connection closed from %s", r.RemoteAddr)
}
