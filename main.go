package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// ---------- Настройки ----------

const (
	maxHistory  = 100            // храним последние 100 сообщений
	historyFile = "history.json" // файл истории
	maxMsgLen   = 512            // максимальная длина сообщения
	writeWait   = 10 * time.Second
	pongWait    = 60 * time.Second
	pingPeriod  = (pongWait * 9) / 10
)

var (
	nameRe       = regexp.MustCompile(`^[a-zA-Z0-9]{2,20}$`)
	errNameTaken = errors.New("имя уже занято")
	upgrader     = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
)

// ---------- Типы данных ----------

// Сообщение чата
type Message struct {
	ID        string   `json:"id"`
	Username  string   `json:"username"`
	Content   string   `json:"content"`
	ReplyTo   *Message `json:"replyTo,omitempty"` // ответ на сообщение
	Timestamp int64    `json:"timestamp"`         // unix ms
}

// То, что присылает клиент
type Incoming struct {
	Content string `json:"content"`
	ReplyTo string `json:"replyTo"` // id сообщения или пусто
}

// Серверный сообщения клиенту
type Envelope struct {
	Type     string    `json:"type"` // history | message | system | users | error
	Message  *Message  `json:"message,omitempty"`
	Messages []Message `json:"messages,omitempty"`
	Users    []string  `json:"users,omitempty"`
	Content  string    `json:"content,omitempty"`
	Code     string    `json:"code,omitempty"`
}

type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	send     chan Envelope
	username string
}

type registerReq struct {
	client *Client
	done   chan error // результат проверки уникальности имени
}

type broadcastReq struct {
	from    *Client
	text    string
	replyTo string
}

// ---------- Хаб ----------

type Hub struct {
	clients    map[*Client]struct{}
	users      map[string]*Client
	history    []Message
	register   chan registerReq
	unregister chan *Client
	broadcast  chan broadcastReq
}

func newHub() *Hub {
	h := &Hub{
		clients:    make(map[*Client]struct{}),
		users:      make(map[string]*Client),
		register:   make(chan registerReq),
		unregister: make(chan *Client),
		broadcast:  make(chan broadcastReq),
	}
	if data, err := os.ReadFile(historyFile); err == nil {
		var msgs []Message
		if err := json.Unmarshal(data, &msgs); err == nil {
			if len(msgs) > maxHistory {
				msgs = msgs[len(msgs)-maxHistory:]
			}
			h.history = msgs
			log.Printf("Uploaded %d messages from the history", len(msgs))
		} else {
			log.Printf("Error in the file %s: %v", historyFile, err)
		}
	}
	return h
}

func (h *Hub) run() {
	for {
		select {
		case req := <-h.register:
			c := req.client
			key := c.username
			if _, ok := h.users[key]; ok {
				req.done <- errNameTaken
				continue
			}
			h.clients[c] = struct{}{}
			h.users[key] = c

			select {
			case c.send <- Envelope{Type: "history", Messages: h.historySnapshot(), Users: h.userList()}:
			default:
			}
			req.done <- nil
			log.Printf("Joined %s", c.username)
			h.sendSystem("✨ " + c.username + " появляется из тумана")
			h.sendUsers()

		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				delete(h.users, c.username)
				close(c.send)
				log.Printf("Leave %s", c.username)
				h.sendSystem("🌙 " + c.username + " растворяется в тумане")
				h.sendUsers()
			}

		case bm := <-h.broadcast:
			content := strings.TrimSpace(bm.text)
			if content == "" {
				continue
			}
			if r := []rune(content); len(r) > maxMsgLen {
				content = string(r[:maxMsgLen])
			}
			msg := Message{
				ID:        newID(),
				Username:  bm.from.username,
				Content:   content,
				Timestamp: time.Now().UnixMilli(),
			}

			if bm.replyTo != "" {
				if orig := h.findMessage(bm.replyTo); orig != nil {
					snapshot := *orig
					snapshot.ReplyTo = nil
					msg.ReplyTo = &snapshot
				}
			}
			h.history = append(h.history, msg)
			if len(h.history) > maxHistory {
				h.history = h.history[len(h.history)-maxHistory:]
			}
			h.saveHistory()
			h.sendToAll(Envelope{Type: "message", Message: &msg})
		}
	}
}

// Рассылает сообщения всем; медленных клиентов отключаем
func (h *Hub) sendToAll(e Envelope) {
	for c := range h.clients {
		select {
		case c.send <- e:
		default:
			delete(h.clients, c)
			delete(h.users, c.username)
			close(c.send)
		}
	}
}

func (h *Hub) sendSystem(text string) {
	h.sendToAll(Envelope{Type: "system", Content: text})
}

func (h *Hub) sendUsers() {
	h.sendToAll(Envelope{Type: "users", Users: h.userList()})
}

func (h *Hub) userList() []string {
	names := make([]string, 0, len(h.users))
	for _, c := range h.users {
		names = append(names, c.username)
	}
	sort.Strings(names)
	return names
}

func (h *Hub) findMessage(id string) *Message {
	for i := len(h.history) - 1; i >= 0; i-- {
		if h.history[i].ID == id {
			return &h.history[i]
		}
	}
	return nil
}

func (h *Hub) historySnapshot() []Message {
	return append([]Message(nil), h.history...)
}

func (h *Hub) saveHistory() {
	data, err := json.MarshalIndent(h.history, "", "	")
	if err != nil {
		log.Println("Error JSON Marshal indent:", err)
		return
	}
	if err := os.WriteFile(historyFile, data, 0644); err != nil {
		log.Println("Error write file history:", err)
	}
}

func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// Пишем клиенту
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(8192)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		var in Incoming
		if err := c.conn.ReadJSON(&in); err != nil {
			break
		}
		c.hub.broadcast <- broadcastReq{from: c, text: in.Content, replyTo: in.ReplyTo}
	}
}

// Клиент пишет
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case env, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "close"))
				return
			}
			if err := c.conn.WriteJSON(env); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ---------- HTTP ----------

func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	username := strings.TrimSpace(r.URL.Query().Get("name"))
	if !nameRe.MatchString(username) {
		http.Error(w, "bad name", http.StatusBadRequest)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	client := &Client{
		hub:      hub,
		conn:     conn,
		send:     make(chan Envelope, 256),
		username: username,
	}
	done := make(chan error, 1)
	hub.register <- registerReq{client: client, done: done}
	if err := <-done; err != nil {
		conn.SetWriteDeadline(time.Now().Add(writeWait))
		conn.WriteJSON(Envelope{
			Type:    "error",
			Code:    "name_taken",
			Content: "This name is already occupied",
		})
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "name taken"))
		conn.Close()
		return
	}
	go client.writePump()
	go client.readPump()
}

func main() {
	hub := newHub()
	go hub.run()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "index.html")
	})
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	log.Println("🌙 Лунный чат запущен http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
