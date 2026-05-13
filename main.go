package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/coder/websocket"
	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

//go:embed static/index.html
var indexHTML []byte

const (
	sendQueueSize         = 128
	broadcastQueueSize    = 256
	saveQueueSize         = 512
	registerQueueSize     = 64
	unregisterQueueSize   = 64
	loginHistoryLimit     = 40
	loadMoreLimit         = 40
	writeWait             = 10 * time.Second
	pingInterval          = 25 * time.Second
	readAckFlushInterval  = 60 * time.Second
	maxMentionReadEntries = 2000
	maxMessageChars       = 1000
	maxUploadBytes        = 100 << 20
	uploadDir             = "./uploads"
	sessionCookieName     = "chat_session"
	sessionTTL            = 7 * 24 * time.Hour
	bcryptCost            = 10
	loginFailWindow       = 10 * time.Minute
	loginFailMax          = 20
	saveEnqueueTimeout    = 2 * time.Second
)

var nickRegexp = regexp.MustCompile(`^[\p{Han}A-Za-z0-9_-]{2,15}$`)

type Message struct {
	ID                string           `json:"id,omitempty"`
	Type              string           `json:"type"`
	From              string           `json:"from"`
	To                string           `json:"to"`
	Content           string           `json:"content"`
	Timestamp         int64            `json:"timestamp"`
	Password          string           `json:"password,omitempty"`
	UserList          []UserStatus     `json:"user_list,omitempty"`
	Mentions          []string         `json:"mentions,omitempty"`
	Messages          []Message        `json:"messages,omitempty"`
	HasMore           bool             `json:"has_more,omitempty"`
	UnreadMap         map[string]int   `json:"unread_map,omitempty"`
	LastReadMap       map[string]int64 `json:"last_read_map,omitempty"`
	PrivateHasMoreMap map[string]bool  `json:"private_has_more_map,omitempty"`
	PublicHasMore     bool             `json:"public_has_more,omitempty"`
	Seq               int64            `json:"seq,omitempty"`
	DBID              int64            `json:"db_id,omitempty"`
	FileName          string           `json:"file_name,omitempty"`
	FileURL           string           `json:"file_url,omitempty"`
	FileSize          int64            `json:"file_size,omitempty"`
	FileMime          string           `json:"file_mime,omitempty"`
}

type UserStatus struct {
	Name   string `json:"name"`
	Online bool   `json:"online"`
}

type Client struct {
	conn             *websocket.Conn
	nick             string
	inviteCode       string
	send             chan Message
	cancel           context.CancelFunc
	mu               sync.Mutex
	lastReadMap      map[string]int64
	mentionReadMap   map[string]bool
	dirtyLastRead    bool
	dirtyMentionRead bool
	closed           bool
}

type Hub struct {
	clients       map[*Client]bool
	nickMap       map[string]*Client
	allUsers      map[string]struct{}
	broadcast     chan Message
	register      chan *Client
	unregister    chan *Client
	saveChan      chan Message
	userListDirty chan struct{}
	mu            sync.RWMutex
}

var (
	hub             *Hub
	db              *sql.DB
	serverPort      string
	listenAddr      string
	r2Client        *s3.Client
	r2Bucket        string
	r2PublicBaseURL string
	useR2Storage    bool
	sessionKey      []byte
	allowedOrigins  []string

	loginFailMu sync.Mutex
	loginFails  = map[string][]time.Time{}

	shuttingDown atomic.Bool
)

type AppConfig struct {
	Port              string   `json:"port"`
	ListenAddr        string   `json:"listen_addr"`
	StorageType       string   `json:"storage_type"`
	R2AccountID       string   `json:"r2_account_id"`
	R2AccessKeyID     string   `json:"r2_access_key_id"`
	R2SecretAccessKey string   `json:"r2_secret_access_key"`
	R2Bucket          string   `json:"r2_bucket"`
	R2PublicBaseURL   string   `json:"r2_public_base_url"`
	DatabasePath      string   `json:"database_path"`
	SessionKey        string   `json:"session_key"`
	AllowedOrigins    []string `json:"allowed_origins"`
}

// ============================================================
// 配置与环境
// ============================================================

func setEnvIfMissing(key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	if strings.TrimSpace(os.Getenv(key)) != "" {
		return
	}
	_ = os.Setenv(key, value)
}

func loadConfigFile() {
	configPath := strings.TrimSpace(os.Getenv("CONFIG_PATH"))
	if configPath == "" {
		configPath = "./config.json"
	}

	info, statErr := os.Stat(configPath)
	if statErr == nil {
		if info.Mode().Perm()&0o077 != 0 {
			log.Printf("warn: %s permissions are too open (%v); recommended chmod 600", configPath, info.Mode().Perm())
		}
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("read config warn: %v", err)
		}
		return
	}

	var cfg AppConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Printf("parse config warn: %v", err)
		return
	}

	setEnvIfMissing("PORT", cfg.Port)
	setEnvIfMissing("LISTEN_ADDR", cfg.ListenAddr)
	setEnvIfMissing("STORAGE_TYPE", cfg.StorageType)
	setEnvIfMissing("R2_ACCOUNT_ID", cfg.R2AccountID)
	setEnvIfMissing("R2_ACCESS_KEY_ID", cfg.R2AccessKeyID)
	setEnvIfMissing("R2_SECRET_ACCESS_KEY", cfg.R2SecretAccessKey)
	setEnvIfMissing("R2_BUCKET", cfg.R2Bucket)
	setEnvIfMissing("R2_PUBLIC_BASE_URL", cfg.R2PublicBaseURL)
	setEnvIfMissing("DATABASE_PATH", cfg.DatabasePath)
	setEnvIfMissing("SESSION_KEY", cfg.SessionKey)

	if len(cfg.AllowedOrigins) > 0 && strings.TrimSpace(os.Getenv("ALLOWED_ORIGINS")) == "" {
		_ = os.Setenv("ALLOWED_ORIGINS", strings.Join(cfg.AllowedOrigins, ","))
	}

	log.Printf("Loaded config from %s", configPath)
}

func initSessionKey() {
	raw := strings.TrimSpace(os.Getenv("SESSION_KEY"))
	if raw != "" {
		if b, err := base64.StdEncoding.DecodeString(raw); err == nil && len(b) >= 32 {
			sessionKey = b
			return
		}
		if len(raw) >= 32 {
			sessionKey = []byte(raw)
			return
		}
		log.Println("warn: SESSION_KEY too short or invalid, generating ephemeral key")
	}

	sessionKey = make([]byte, 32)
	if _, err := rand.Read(sessionKey); err != nil {
		log.Fatal("generate session key:", err)
	}
	log.Println("warn: using ephemeral SESSION_KEY; sessions invalidated on restart. Set SESSION_KEY env to persist.")
}

func initAllowedOrigins() {
	raw := strings.TrimSpace(os.Getenv("ALLOWED_ORIGINS"))
	if raw == "" {
		allowedOrigins = nil
		return
	}
	parts := strings.Split(raw, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			allowedOrigins = append(allowedOrigins, strings.ToLower(p))
		}
	}
}

// ============================================================
// Hub
// ============================================================

func initHub() *Hub {
	return &Hub{
		clients:       make(map[*Client]bool),
		nickMap:       make(map[string]*Client),
		allUsers:      make(map[string]struct{}),
		broadcast:     make(chan Message, broadcastQueueSize),
		register:      make(chan *Client, registerQueueSize),
		unregister:    make(chan *Client, unregisterQueueSize),
		saveChan:      make(chan Message, saveQueueSize),
		userListDirty: make(chan struct{}, 1),
	}
}

func (h *Hub) removeClientLocked(client *Client) string {
	removedNick := ""
	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		if client.nick != "" {
			if existing, ok := h.nickMap[client.nick]; ok && existing == client {
				delete(h.nickMap, client.nick)
				removedNick = client.nick
			}
		}
		client.mu.Lock()
		if !client.closed {
			client.closed = true
			close(client.send)
		}
		client.mu.Unlock()
		client.cancel()
	}
	return removedNick
}

func (h *Hub) markUserListDirty() {
	select {
	case h.userListDirty <- struct{}{}:
	default:
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()

		case client := <-h.unregister:
			h.mu.Lock()
			removedNick := h.removeClientLocked(client)
			h.mu.Unlock()

			if removedNick != "" {
				go flushClientReadState(client)
				h.markUserListDirty()
			}

		case msg := <-h.broadcast:
			h.mu.RLock()
			snapshot := make([]*Client, 0, len(h.clients))
			for client := range h.clients {
				snapshot = append(snapshot, client)
			}
			h.mu.RUnlock()

			var dead []*Client
			for _, client := range snapshot {
				if !safeSend(client, msg) {
					dead = append(dead, client)
				}
			}
			if len(dead) > 0 {
				h.mu.Lock()
				dirty := false
				for _, c := range dead {
					if nick := h.removeClientLocked(c); nick != "" {
						dirty = true
						go flushClientReadState(c)
					}
				}
				h.mu.Unlock()
				if dirty {
					h.markUserListDirty()
				}
			}
		}
	}
}

// ============================================================
// 密码 / 哈希 / 校验
// ============================================================

func legacySha256(pwd string) string {
	h := sha256.New()
	h.Write([]byte(pwd))
	return hex.EncodeToString(h.Sum(nil))
}

func hashPasswordBcrypt(pwd string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(pwd), bcryptCost)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func verifyPassword(stored, plain string) (ok bool, isLegacy bool) {
	if stored == "" {
		return false, false
	}
	if strings.HasPrefix(stored, "\$2a$") || strings.HasPrefix(stored, "\$2b$") || strings.HasPrefix(stored, "\$2y$") {
		err := bcrypt.CompareHashAndPassword([]byte(stored), []byte(plain))
		return err == nil, false
	}
	// legacy sha256 hex
	return hmac.Equal([]byte(stored), []byte(legacySha256(plain))), true
}

func upgradePasswordHash(nick, plain string) {
	newHash, err := hashPasswordBcrypt(plain)
	if err != nil {
		log.Println("upgrade hash error:", err)
		return
	}
	if _, err := db.Exec("UPDATE users SET password = ? WHERE username = ?", newHash, nick); err != nil {
		log.Println("upgrade hash db error:", err)
	}
}

func generateMessageID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("msg_%d_%s", time.Now().UnixMilli(), hex.EncodeToString(b[:]))
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// ============================================================
// Session cookie (HMAC signed: nick|expUnix|sig)
// ============================================================

func signSession(nick string, exp int64) string {
	payload := fmt.Sprintf("%s|%d", nick, exp)
	mac := hmac.New(sha256.New, sessionKey)
	mac.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + sig
}

func verifySession(token string) (nick string, ok bool) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return "", false
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", false
	}
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false
	}
	mac := hmac.New(sha256.New, sessionKey)
	mac.Write(payloadBytes)
	if !hmac.Equal(mac.Sum(nil), sigBytes) {
		return "", false
	}
	fields := strings.SplitN(string(payloadBytes), "|", 2)
	if len(fields) != 2 {
		return "", false
	}
	expUnix, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return "", false
	}
	if time.Now().Unix() > expUnix {
		return "", false
	}
	return fields[0], true
}

func setSessionCookie(w http.ResponseWriter, r *http.Request, nick string) {
	exp := time.Now().Add(sessionTTL).Unix()
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    signSession(nick, exp),
		Path:     "/",
		MaxAge:   int(sessionTTL.Seconds()),
		HttpOnly: true,
		Secure:   isHTTPSRequest(r),
		SameSite: http.SameSiteLaxMode,
	})
}

func sessionNickFromRequest(r *http.Request) (string, bool) {
	c, err := r.Cookie(sessionCookieName)
	if err != nil || c == nil {
		return "", false
	}
	nick, ok := verifySession(c.Value)
	if !ok {
		return "", false
	}
	if !validateNick(nick) {
		return "", false
	}
	if !userExists(nick) {
		return "", false
	}
	return nick, true
}

// ============================================================
// Mention 解析（服务端为唯一权威）
// ============================================================

func parseMentions(content string, usernames []string, sender string) []string {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil
	}

	candidates := make([]string, 0, len(usernames))
	for _, name := range usernames {
		name = strings.TrimSpace(name)
		if name == "" || name == sender {
			continue
		}
		candidates = append(candidates, name)
	}

	sort.Slice(candidates, func(i, j int) bool {
		return len([]rune(candidates[i])) > len([]rune(candidates[j]))
	})

	seen := make(map[string]bool)
	var result []string
	runes := []rune(content)

	for i := 0; i < len(runes); i++ {
		if runes[i] != '@' {
			continue
		}
		if i > 0 && !isWhitespaceRune(runes[i-1]) {
			continue
		}

		rest := string(runes[i+1:])
		for _, name := range candidates {
			if strings.HasPrefix(rest, name) {
				nextPos := i + 1 + len([]rune(name))
				var nextRune rune
				if nextPos < len(runes) {
					nextRune = runes[nextPos]
				}
				if nextPos == len(runes) || isMentionBoundary(nextRune) {
					if !seen[name] {
						seen[name] = true
						result = append(result, name)
					}
					break
				}
			}
		}
	}

	return result
}

func isWhitespaceRune(r rune) bool {
	return r == ' ' || r == '\n' || r == '\t' || r == '\r'
}

func isMentionBoundary(r rune) bool {
	if r == 0 {
		return true
	}
	switch r {
	case ' ', '\n', '\t', '\r', ',', '，', '.', '。', '!', '！', '?', '？', ':', '：', ';', '；':
		return true
	default:
		return false
	}
}

func mentionsToJSON(mentions []string) string {
	if len(mentions) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(mentions)
	return string(b)
}

func parseMentionsJSON(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var mentions []string
	_ = json.Unmarshal([]byte(raw), &mentions)
	return mentions
}

func parseLastReadJSON(raw string) map[string]int64 {
	if strings.TrimSpace(raw) == "" {
		return map[string]int64{}
	}
	var m map[string]int64
	if err := json.Unmarshal([]byte(raw), &m); err != nil || m == nil {
		return map[string]int64{}
	}
	return m
}

func lastReadToJSON(m map[string]int64) string {
	if m == nil {
		m = map[string]int64{}
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func parseMentionReadJSON(raw string) map[string]bool {
	if strings.TrimSpace(raw) == "" {
		return map[string]bool{}
	}
	var m map[string]bool
	if err := json.Unmarshal([]byte(raw), &m); err != nil || m == nil {
		return map[string]bool{}
	}
	return trimMentionReadMap(m, maxMentionReadEntries)
}

func mentionReadToJSON(m map[string]bool) string {
	if m == nil {
		m = map[string]bool{}
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func trimMentionReadMap(m map[string]bool, max int) map[string]bool {
	if len(m) <= max {
		return m
	}
	
	n := make(map[string]bool, max)
	i := 0
	for k, v := range m {
		n[k] = v
		i++
		if i >= max {
			break
		}
	}
	return n
}

func validateNick(nick string) bool {
	return nickRegexp.MatchString(nick)
}

func normalizeContent(content string) (string, bool) {
	content = strings.TrimSpace(content)
	if content == "" {
		return "", false
	}
	if len([]rune(content)) > maxMessageChars {
		return "", false
	}
	return content, true
}

func safeSend(c *Client, msg Message) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return false
	}

	select {
	case c.send <- msg:
		return true
	default:
		return false
	}
}

// ============================================================
// 在线列表广播
// ============================================================

func buildUserListSnapshot() []UserStatus {
	hub.mu.RLock()
	names := make([]string, 0, len(hub.allUsers))
	for name := range hub.allUsers {
		names = append(names, name)
	}
	sort.Strings(names)

	nickMap := make(map[string]*Client, len(hub.nickMap))
	for k, v := range hub.nickMap {
		nickMap[k] = v
	}
	hub.mu.RUnlock()

	list := make([]UserStatus, 0, len(names))
	for _, name := range names {
		_, isOnline := nickMap[name]
		list = append(list, UserStatus{Name: name, Online: isOnline})
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].Online != list[j].Online {
			return list[i].Online
		}
		return list[i].Name < list[j].Name
	})

	return list
}

func broadcastUserList() {
	list := buildUserListSnapshot()
	select {
	case hub.broadcast <- Message{
		Type:      "online",
		UserList:  list,
		Timestamp: time.Now().Unix(),
	}:
	default:
		log.Println("warn: broadcast queue full, dropping online update")
	}
}

// ============================================================
// 持久化
// ============================================================

func saveMessage(msg Message) {
	switch msg.Type {
	case "system", "online", "read_sync", "mention_read_sync", "history_done", "history_page", "pong", "ping", "unread_sync":
		return
	}

	_, err := db.Exec(
		"INSERT INTO messages (msg_id, type, sender, receiver, content, timestamp, mentions, file_name, file_url, file_size, file_mime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		msg.ID, msg.Type, msg.From, msg.To, msg.Content, msg.Timestamp, mentionsToJSON(msg.Mentions), msg.FileName, msg.FileURL, msg.FileSize, msg.FileMime,
	)
	if err != nil {
		log.Println("DB Error:", err)
	}
}

func startSaveWorker() {
	go func() {
		for msg := range hub.saveChan {
			saveMessage(msg)
		}
	}()
}

func enqueueSave(msg Message) {
	select {
	case hub.saveChan <- msg:
		return
	default:
	}
	timer := time.NewTimer(saveEnqueueTimeout)
	defer timer.Stop()
	select {
	case hub.saveChan <- msg:
	case <-timer.C:
		log.Printf("warn: save queue full, dropping message id=%s type=%s", msg.ID, msg.Type)
	}
}

func getAllUsernamesFromMemory() []string {
	hub.mu.RLock()
	defer hub.mu.RUnlock()

	names := make([]string, 0, len(hub.allUsers))
	for name := range hub.allUsers {
		names = append(names, name)
	}
	return names
}

func userExists(username string) bool {
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	_, ok := hub.allUsers[username]
	return ok
}

// 一条 SQL 算出全部对等方未读，避免 N+1。
func computeUnreadMap(lastRead map[string]int64, me string) map[string]int {
	result := make(map[string]int)

	publicLast := lastRead["public"]
	var publicCount int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM messages WHERE type='public' AND timestamp > ?",
		publicLast,
	).Scan(&publicCount); err == nil && publicCount > 0 {
		result["public"] = publicCount
	}

	rows, err := db.Query(
		"SELECT sender, timestamp FROM messages WHERE type='private' AND receiver=?",
		me,
	)
	if err != nil {
		log.Println("compute unread err:", err)
		return result
	}
	defer rows.Close()

	for rows.Next() {
		var sender string
		var ts int64
		if err := rows.Scan(&sender, &ts); err != nil {
			continue
		}
		if ts > lastRead[sender] {
			result[sender]++
		}
	}
	return result
}

func loadInitialHistory(nick string) ([]Message, bool, map[string]bool, error) {
	rows, err := db.Query(`
		SELECT id, msg_id, type, sender, receiver, content, timestamp, mentions, file_name, file_url, file_size, file_mime
		FROM messages
		WHERE type='public' OR (type='private' AND (sender=? OR receiver=?))
		ORDER BY timestamp DESC, id DESC
		LIMIT ?
	`, nick, nick, loginHistoryLimit)
	if err != nil {
		return nil, false, nil, err
	}
	defer rows.Close()

	msgs := make([]Message, 0, loginHistoryLimit)
	for rows.Next() {
		var m Message
		var mentionsRaw string
		if err := rows.Scan(&m.DBID, &m.ID, &m.Type, &m.From, &m.To, &m.Content, &m.Timestamp, &mentionsRaw, &m.FileName, &m.FileURL, &m.FileSize, &m.FileMime); err != nil {
			continue
		}
		m.Mentions = parseMentionsJSON(mentionsRaw)
		m.Seq = m.DBID
		msgs = append(msgs, m)
	}

	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}

	publicHasMore := false
	privateHasMore := make(map[string]bool)

	if len(msgs) == 0 {
		return msgs, false, privateHasMore, nil
	}

	var publicMinID int64
	privateMinIDMap := make(map[string]int64)

	for _, m := range msgs {
		if m.Type == "public" {
			if publicMinID == 0 || m.DBID < publicMinID {
				publicMinID = m.DBID
			}
			continue
		}

		peer := m.From
		if peer == nick {
			peer = m.To
		}
		if old, ok := privateMinIDMap[peer]; !ok || m.DBID < old {
			privateMinIDMap[peer] = m.DBID
		}
	}

	if publicMinID > 0 {
		var exists int
		_ = db.QueryRow(
			"SELECT EXISTS(SELECT 1 FROM messages WHERE type='public' AND id < ? LIMIT 1)",
			publicMinID,
		).Scan(&exists)
		publicHasMore = exists == 1
	}

	for peer, minID := range privateMinIDMap {
		var exists int
		_ = db.QueryRow(`
			SELECT EXISTS(SELECT 1 FROM messages
			WHERE type='private'
			  AND ((sender=? AND receiver=?) OR (sender=? AND receiver=?))
			  AND id < ?)
		`, nick, peer, peer, nick, minID).Scan(&exists)
		privateHasMore[peer] = exists == 1
	}

	return msgs, publicHasMore, privateHasMore, nil
}

func flushClientReadState(c *Client) {
	if c == nil || c.nick == "" {
		return
	}

	c.mu.Lock()
	if !c.dirtyLastRead && !c.dirtyMentionRead {
		c.mu.Unlock()
		return
	}

	lastReadJSON := lastReadToJSON(c.lastReadMap)
	mentionReadJSON := mentionReadToJSON(trimMentionReadMap(c.mentionReadMap, maxMentionReadEntries))
	dirtyLast := c.dirtyLastRead
	dirtyMention := c.dirtyMentionRead
	c.dirtyLastRead = false
	c.dirtyMentionRead = false
	nick := c.nick
	c.mu.Unlock()

	switch {
	case dirtyLast && dirtyMention:
		_, err := db.Exec("UPDATE users SET last_read_at = ?, mention_read_at = ? WHERE username = ?", lastReadJSON, mentionReadJSON, nick)
		if err != nil {
			log.Println("flush read state error:", err)
		}
	case dirtyLast:
		_, err := db.Exec("UPDATE users SET last_read_at = ? WHERE username = ?", lastReadJSON, nick)
		if err != nil {
			log.Println("flush last_read error:", err)
		}
	case dirtyMention:
		_, err := db.Exec("UPDATE users SET mention_read_at = ? WHERE username = ?", mentionReadJSON, nick)
		if err != nil {
			log.Println("flush mention_read error:", err)
		}
	}
}

func flushAllReadStates() {
	hub.mu.RLock()
	clients := make([]*Client, 0, len(hub.clients))
	for c := range hub.clients {
		clients = append(clients, c)
	}
	hub.mu.RUnlock()

	for _, c := range clients {
		flushClientReadState(c)
	}
}

func startReadStateFlusher() {
	go func() {
		ticker := time.NewTicker(readAckFlushInterval)
		defer ticker.Stop()
		for range ticker.C {
			flushAllReadStates()
		}
	}()
}

func startUserListBroadcaster() {
	go func() {
		for range hub.userListDirty {
			broadcastUserList()
			time.Sleep(50 * time.Millisecond)
		}
	}()
}

// ============================================================
// 登录失败限流
// ============================================================

func clientIP(r *http.Request) string {
	if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
		if idx := strings.IndexByte(xf, ','); idx >= 0 {
			return strings.TrimSpace(xf[:idx])
		}
		return strings.TrimSpace(xf)
	}
	if ip := r.Header.Get("X-Real-IP"); ip != "" {
		return strings.TrimSpace(ip)
	}
	host := r.RemoteAddr
	if i := strings.LastIndexByte(host, ':'); i > 0 {
		host = host[:i]
	}
	return host
}

func recordLoginFailure(key string) (blocked bool) {
	if key == "" {
		return false
	}
	now := time.Now()
	loginFailMu.Lock()
	defer loginFailMu.Unlock()

	cutoff := now.Add(-loginFailWindow)
	arr := loginFails[key]
	kept := arr[:0]
	for _, t := range arr {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	kept = append(kept, now)
	loginFails[key] = kept
	return len(kept) >= loginFailMax
}

func isLoginBlocked(key string) bool {
	if key == "" {
		return false
	}
	now := time.Now()
	loginFailMu.Lock()
	defer loginFailMu.Unlock()
	cutoff := now.Add(-loginFailWindow)
	arr := loginFails[key]
	count := 0
	for _, t := range arr {
		if t.After(cutoff) {
			count++
		}
	}
	return count >= loginFailMax
}

func clearLoginFailures(key string) {
	if key == "" {
		return
	}
	loginFailMu.Lock()
	delete(loginFails, key)
	loginFailMu.Unlock()
}

// ============================================================
// WebSocket 处理
// ============================================================

func (c *Client) handleNick(msg Message, ip string, sessionWriter chan<- string) {
	nick := strings.TrimSpace(msg.From)
	plain := msg.Password

	if !validateNick(nick) {
		_ = safeSend(c, Message{Type: "system", Content: "昵称仅支持2-15位中文、字母、数字、下划线或中划线"})
		return
	}

	rateKey := ip + "|" + nick
	if isLoginBlocked(rateKey) {
		_ = safeSend(c, Message{Type: "system", Content: "登录尝试过于频繁，请稍后再试"})
		time.Sleep(800 * time.Millisecond)
		c.cancel()
		return
	}

	var storedPwd string
	var lastReadJSON, mentionReadJSON string

	err := db.QueryRow("SELECT password, last_read_at, mention_read_at FROM users WHERE username = ?", nick).
		Scan(&storedPwd, &lastReadJSON, &mentionReadJSON)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		hashed, herr := hashPasswordBcrypt(plain)
		if herr != nil {
			log.Println("hash password error:", herr)
			_ = safeSend(c, Message{Type: "system", Content: "登录失败，请稍后重试"})
			return
		}
		_, insErr := db.Exec(
			"INSERT INTO users (username, password, last_read_at, mention_read_at) VALUES (?, ?, '{}', '{}')",
			nick, hashed,
		)
		if insErr != nil {
			log.Println("insert user error:", insErr)
			// 并发同名注册，再读一次
			err = db.QueryRow("SELECT password, last_read_at, mention_read_at FROM users WHERE username = ?", nick).
				Scan(&storedPwd, &lastReadJSON, &mentionReadJSON)
			if err != nil {
				_ = safeSend(c, Message{Type: "system", Content: "登录失败，请稍后重试"})
				return
			}
			okPwd, legacy := verifyPassword(storedPwd, plain)
			if !okPwd {
				recordLoginFailure(rateKey)
				time.Sleep(500 * time.Millisecond)
				_ = safeSend(c, Message{Type: "system", Content: "密码错误或昵称被占用"})
				return
			}
			if legacy {
				go upgradePasswordHash(nick, plain)
			}
		} else {
			storedPwd = hashed
			lastReadJSON = "{}"
			mentionReadJSON = "{}"
		}
	case err != nil:
		log.Println("query user error:", err)
		_ = safeSend(c, Message{Type: "system", Content: "登录失败，请稍后重试"})
		return
	default:
		okPwd, legacy := verifyPassword(storedPwd, plain)
		if !okPwd {
			recordLoginFailure(rateKey)
			time.Sleep(500 * time.Millisecond)
			_ = safeSend(c, Message{Type: "system", Content: "密码错误或昵称被占用"})
			return
		}
		if legacy {
			go upgradePasswordHash(nick, plain)
		}
	}

	clearLoginFailures(rateKey)

	if err := bindInviteCodeToNick(c.inviteCode, nick); err != nil {
		_ = safeSend(c, Message{Type: "system", Content: err.Error()})
		time.Sleep(300 * time.Millisecond)
		c.cancel()
		return
	}

	var oldClient *Client

	hub.mu.Lock()
	if existing, online := hub.nickMap[nick]; online && existing != c {
		oldClient = existing
	}
	c.nick = nick
	hub.nickMap[nick] = c
	hub.allUsers[nick] = struct{}{}
	hub.mu.Unlock()

	if oldClient != nil {
		_ = safeSend(oldClient, Message{Type: "system", Content: "你的账号已在新连接登录，本连接已下线"})
		go func() {
			hub.unregister <- oldClient
		}()
	}

	c.mu.Lock()
	c.lastReadMap = parseLastReadJSON(lastReadJSON)
	c.mentionReadMap = parseMentionReadJSON(mentionReadJSON)
	c.mu.Unlock()

	// 通知 HTTP 层下发 session cookie（通过同源升级请求时已经写入，这里走另一条通道）
	// 实际我们在 /ws upgrade 之前/之后无法再写 header，所以走轮询 endpoint /session 来获取——
	// 但为了减少前端改动，这里把签发的 token 也通过 WS 消息回传，前端可选择忽略。
	exp := time.Now().Add(sessionTTL).Unix()
	token := signSession(nick, exp)
	select {
	case sessionWriter <- token:
	default:
	}
	_ = safeSend(c, Message{
		Type:    "session",
		Content: token,
	})

	_ = safeSend(c, Message{
		Type:        "read_sync",
		Content:     lastReadToJSON(c.lastReadMap),
		LastReadMap: c.lastReadMap,
	})
	_ = safeSend(c, Message{
		Type:    "mention_read_sync",
		Content: mentionReadToJSON(c.mentionReadMap),
	})

	unreadMap := computeUnreadMap(c.lastReadMap, c.nick)
	_ = safeSend(c, Message{
		Type:      "unread_sync",
		UnreadMap: unreadMap,
	})

	go c.loadHistoryAndSync()
	hub.markUserListDirty()
}

func (c *Client) loadHistoryAndSync() {
	if c.nick == "" {
		return
	}

	msgs, publicHasMore, privateHasMore, err := loadInitialHistory(c.nick)
	if err != nil {
		log.Println("load history error:", err)
		_ = safeSend(c, Message{Type: "history_done"})
		return
	}

	_ = safeSend(c, Message{
		Type:              "history_done",
		Messages:          msgs,
		PublicHasMore:     publicHasMore,
		PrivateHasMoreMap: privateHasMore,
	})
}

func (c *Client) handleReadAck(msg Message) {
	if c.nick == "" {
		return
	}
	target := strings.TrimSpace(msg.To)
	if target == "" {
		target = "public"
	}
	c.mu.Lock()
	if c.lastReadMap == nil {
		c.lastReadMap = make(map[string]int64)
	}
	c.lastReadMap[target] = time.Now().Unix()
	c.dirtyLastRead = true
	c.mu.Unlock()
}

func (c *Client) handleMentionReadAck(msg Message) {
	if c.nick == "" {
		return
	}
	mentionMsgID := strings.TrimSpace(msg.Content)
	if mentionMsgID == "" {
		return
	}
	c.mu.Lock()
	if c.mentionReadMap == nil {
		c.mentionReadMap = make(map[string]bool)
	}
	c.mentionReadMap[mentionMsgID] = true
	c.mentionReadMap = trimMentionReadMap(c.mentionReadMap, maxMentionReadEntries)
	c.dirtyMentionRead = true
	c.mu.Unlock()
}

func (c *Client) handlePublic(msg Message) {
	if c.nick == "" {
		return
	}

	content, ok := normalizeContent(msg.Content)
	if !ok && msg.FileURL == "" {
		_ = safeSend(c, Message{Type: "system", Content: fmt.Sprintf("消息不能为空且不能超过%d个字符", maxMessageChars)})
		return
	}

	msg.ID = generateMessageID()
	msg.From = c.nick
	msg.Timestamp = time.Now().Unix()
	msg.Content = content

	usernames := getAllUsernamesFromMemory()
	msg.Mentions = parseMentions(msg.Content, usernames, c.nick) // 服务端权威

	// 文件字段：仅信任 file_url 起始格式（本地 /files/ 或 R2 公开前缀）
	if msg.FileURL != "" && !isAllowedFileURL(msg.FileURL) {
		msg.FileURL = ""
		msg.FileName = ""
		msg.FileSize = 0
		msg.FileMime = ""
	}

	select {
	case hub.broadcast <- msg:
	default:
		_ = safeSend(c, Message{Type: "system", Content: "服务器繁忙，请稍后再试"})
		return
	}
	enqueueSave(msg)
}

func (c *Client) handlePrivate(msg Message) {
	if c.nick == "" {
		return
	}

	targetUser := strings.TrimSpace(msg.To)
	if targetUser == "" || targetUser == c.nick {
		_ = safeSend(c, Message{Type: "system", Content: "私聊目标无效"})
		return
	}
	if !userExists(targetUser) {
		_ = safeSend(c, Message{Type: "system", Content: "目标用户不存在"})
		return
	}

	content, ok := normalizeContent(msg.Content)
	if !ok && msg.FileURL == "" {
		_ = safeSend(c, Message{Type: "system", Content: fmt.Sprintf("消息不能为空且不能超过%d个字符", maxMessageChars)})
		return
	}

	msg.ID = generateMessageID()
	msg.From = c.nick
	msg.To = targetUser
	msg.Timestamp = time.Now().Unix()
	msg.Content = content
	msg.Mentions = nil

	if msg.FileURL != "" && !isAllowedFileURL(msg.FileURL) {
		msg.FileURL = ""
		msg.FileName = ""
		msg.FileSize = 0
		msg.FileMime = ""
	}

	_ = safeSend(c, msg)

	hub.mu.RLock()
	target, exists := hub.nickMap[msg.To]
	hub.mu.RUnlock()

	if exists && target != c {
		_ = safeSend(target, msg)
	}

	enqueueSave(msg)
}

func isAllowedFileURL(u string) bool {
	if strings.HasPrefix(u, "/files/") {
		return true
	}
	if r2PublicBaseURL != "" && strings.HasPrefix(u, r2PublicBaseURL+"/") {
		return true
	}
	return false

}

func (c *Client) handleLoadMore(msg Message) {
	if c.nick == "" {
		return
	}

	chat := "public"
	if strings.TrimSpace(msg.To) != "" {
		chat = strings.TrimSpace(msg.To)
	}

	beforeTs := msg.Timestamp
	beforeSeq := msg.Seq
	if beforeTs <= 0 {
		beforeTs = time.Now().Unix() + 1
	}
	if beforeSeq <= 0 {
		beforeSeq = 1 << 62
	}

	var rows *sql.Rows
	var err error

	if chat == "public" {
		rows, err = db.Query(`
			SELECT id, msg_id, type, sender, receiver, content, timestamp, mentions, file_name, file_url, file_size, file_mime
			FROM messages
			WHERE type='public'
			  AND (timestamp < ? OR (timestamp = ? AND id < ?))
			ORDER BY timestamp DESC, id DESC
			LIMIT ?
		`, beforeTs, beforeTs, beforeSeq, loadMoreLimit)
	} else {
		if !userExists(chat) {
			_ = safeSend(c, Message{Type: "system", Content: "目标用户不存在"})
			return
		}

		rows, err = db.Query(`
			SELECT id, msg_id, type, sender, receiver, content, timestamp, mentions, file_name, file_url, file_size, file_mime
			FROM messages
			WHERE type='private'
			  AND ((sender=? AND receiver=?) OR (sender=? AND receiver=?))
			  AND (timestamp < ? OR (timestamp = ? AND id < ?))
			ORDER BY timestamp DESC, id DESC
			LIMIT ?
		`, c.nick, chat, chat, c.nick, beforeTs, beforeTs, beforeSeq, loadMoreLimit)
	}

	if err != nil {
		log.Println("load more error:", err)
		_ = safeSend(c, Message{Type: "history_page", To: chat, HasMore: false})
		return
	}
	defer rows.Close()

	msgs := make([]Message, 0, loadMoreLimit)
	for rows.Next() {
		var m Message
		var mentionsRaw string
		if err := rows.Scan(&m.DBID, &m.ID, &m.Type, &m.From, &m.To, &m.Content, &m.Timestamp, &mentionsRaw, &m.FileName, &m.FileURL, &m.FileSize, &m.FileMime); err != nil {
			continue
		}
		m.Mentions = parseMentionsJSON(mentionsRaw)
		m.Seq = m.DBID
		msgs = append(msgs, m)
	}

	hasMore := false
	if len(msgs) > 0 {
		last := msgs[len(msgs)-1]
		if chat == "public" {
			var exists int
			_ = db.QueryRow(`
				SELECT EXISTS(SELECT 1 FROM messages
				WHERE type='public'
				  AND (timestamp < ? OR (timestamp = ? AND id < ?)))
			`, last.Timestamp, last.Timestamp, last.DBID).Scan(&exists)
			hasMore = exists == 1
		} else {
			var exists int
			_ = db.QueryRow(`
				SELECT EXISTS(SELECT 1 FROM messages
				WHERE type='private'
				  AND ((sender=? AND receiver=?) OR (sender=? AND receiver=?))
				  AND (timestamp < ? OR (timestamp = ? AND id < ?)))
			`, c.nick, chat, chat, c.nick, last.Timestamp, last.Timestamp, last.DBID).Scan(&exists)
			hasMore = exists == 1
		}
	}

	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}

	_ = safeSend(c, Message{
		Type:     "history_page",
		To:       chat,
		Messages: msgs,
		HasMore:  hasMore,
	})
}

func (c *Client) readPump(ctx context.Context, ip string) {
	defer func() {
		hub.unregister <- c
		_ = c.conn.Close(websocket.StatusNormalClosure, "")
	}()

	sessionWriter := make(chan string, 1)

	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}

		if len(data) > (maxMessageChars*4 + 4096) {
			continue
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "ping":
			_ = safeSend(c, Message{Type: "pong", Timestamp: time.Now().Unix()})
		case "nick":
			c.handleNick(msg, ip, sessionWriter)
		case "read_ack":
			c.handleReadAck(msg)
		case "mention_read_ack":
			c.handleMentionReadAck(msg)
		case "public":
			c.handlePublic(msg)
		case "private":
			c.handlePrivate(msg)
		case "load_more":
			c.handleLoadMore(msg)
		}
	}
}

func (c *Client) writePump(ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				return
			}

			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}

			writeCtx, cancel := context.WithTimeout(ctx, writeWait)
			err = c.conn.Write(writeCtx, websocket.MessageText, data)
			cancel()
			if err != nil {
				return
			}

		case <-ticker.C:
			writeCtx, cancel := context.WithTimeout(ctx, writeWait)
			err := c.conn.Ping(writeCtx)
			cancel()
			if err != nil {
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// ============================================================
// 用户表 / 邀请码
// ============================================================

func loadAllUsersToMemory() error {
	rows, err := db.Query("SELECT username FROM users")
	if err != nil {
		return err
	}
	defer rows.Close()

	users := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil && strings.TrimSpace(name) != "" {
			users[name] = struct{}{}
		}
	}

	hub.mu.Lock()
	hub.allUsers = users
	hub.mu.Unlock()
	return nil
}

func importInviteCodesFromFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Println("read invite codes warn:", err)
		}
		return
	}

	lines := strings.Split(string(data), "\n")
	now := time.Now().Unix()
	imported := 0

	for _, line := range lines {
		code := strings.TrimSpace(line)
		if code == "" || strings.HasPrefix(code, "#") {
			continue
		}

		res, err := db.Exec("INSERT OR IGNORE INTO invite_codes(code, created_at) VALUES(?, ?)", code, now)
		if err != nil {
			log.Println("import invite code error:", err)
			continue
		}
		if n, _ := res.RowsAffected(); n > 0 {
			imported++
		}
	}

	if imported > 0 {
		log.Printf("Imported %d invite code(s)", imported)
	}
}

func isInviteCodeValid(code string) bool {
	code = strings.TrimSpace(code)
	if code == "" {
		return false
	}

	var disabled int
	err := db.QueryRow("SELECT disabled FROM invite_codes WHERE code = ?", code).Scan(&disabled)
	if err != nil {
		return false
	}
	return disabled == 0
}

func getInviteCodeFromRequest(r *http.Request) (string, bool) {
	cookie, err := r.Cookie("invite_code")
	if err != nil || cookie == nil {
		return "", false
	}

	code := strings.TrimSpace(cookie.Value)
	if !isInviteCodeValid(code) {
		return "", false
	}
	return code, true
}

func bindInviteCodeToNick(code, nick string) error {
	code = strings.TrimSpace(code)
	nick = strings.TrimSpace(nick)
	if code == "" || nick == "" {
		return errors.New("邀请码无效")
	}

	var usedBy string
	var disabled int
	err := db.QueryRow("SELECT used_by, disabled FROM invite_codes WHERE code = ?", code).Scan(&usedBy, &disabled)
	if err != nil {
		return errors.New("邀请码无效")
	}
	if disabled != 0 {
		return errors.New("邀请码已被禁用")
	}
	if usedBy != "" && usedBy != nick {
		return errors.New("当前邀请码已绑定其他昵称")
	}
	var existingCode string
	err = db.QueryRow("SELECT code FROM invite_codes WHERE used_by = ? AND code <> ? AND disabled = 0 LIMIT 1", nick, code).Scan(&existingCode)
	if err == nil {
		return errors.New("该昵称已绑定其他邀请码")
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if usedBy == nick {
		return nil
	}

	res, err := db.Exec(
		"UPDATE invite_codes SET used_by = ?, used_at = ? WHERE code = ? AND (used_by = '' OR used_by IS NULL)",
		nick, time.Now().Unix(), code,
	)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return errors.New("当前邀请码已绑定其他昵称")
	}
	return nil
}

// ============================================================
// 数据库初始化
// ============================================================

func tableHasColumn(table, column string) bool {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			continue
		}
		if strings.EqualFold(name, column) {
			return true
		}
	}
	return false
}

func ensureColumn(table, column, ddl string) {
	if tableHasColumn(table, column) {
		return
	}
	if _, err := db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", table, ddl)); err != nil {
		log.Printf("warn: add column %s.%s: %v", table, column, err)
	}
}

func setupDatabase() error {
	var err error

	dbPath := strings.TrimSpace(os.Getenv("DATABASE_PATH"))
	if dbPath == "" {
		dbPath = "./chat.db"
	}

	log.Printf("Using database: %s", dbPath)

	db, err = sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_busy_timeout=10000")
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA temp_store=MEMORY;",
		"PRAGMA foreign_keys=ON;",
		"PRAGMA busy_timeout=10000;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			log.Println("pragma warn:", err)
		}
	}

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			username TEXT PRIMARY KEY,
			password TEXT,
			last_read_at TEXT DEFAULT '{}',
			mention_read_at TEXT DEFAULT '{}'
		)`,
		`CREATE TABLE IF NOT EXISTS messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			msg_id TEXT,
			type TEXT,
			sender TEXT,
			receiver TEXT,
			content TEXT,
			timestamp INTEGER,
			mentions TEXT DEFAULT '[]',
			file_name TEXT DEFAULT '',
			file_url TEXT DEFAULT '',
			file_size INTEGER DEFAULT 0,
			file_mime TEXT DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS invite_codes (
			code TEXT PRIMARY KEY,
			used_by TEXT DEFAULT '',
			used_at INTEGER DEFAULT 0,
			created_at INTEGER NOT NULL,
			disabled INTEGER DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(timestamp DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_private ON messages(type, sender, receiver, timestamp DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_msgid ON messages(msg_id)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_public ON messages(type, timestamp DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_receiver_priv ON messages(type, receiver, sender, timestamp DESC)`,
	}

	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	ensureColumn("users", "last_read_at", "last_read_at TEXT DEFAULT '{}'")
	ensureColumn("users", "mention_read_at", "mention_read_at TEXT DEFAULT '{}'")
	ensureColumn("messages", "msg_id", "msg_id TEXT")
	ensureColumn("messages", "mentions", "mentions TEXT DEFAULT '[]'")
	ensureColumn("messages", "file_name", "file_name TEXT DEFAULT ''")
	ensureColumn("messages", "file_url", "file_url TEXT DEFAULT ''")
	ensureColumn("messages", "file_size", "file_size INTEGER DEFAULT 0")
	ensureColumn("messages", "file_mime", "file_mime TEXT DEFAULT ''")

	return nil
}

// ============================================================
// R2 / 上传
// ============================================================

func setupR2Storage(ctx context.Context) error {
	storageType := strings.ToLower(strings.TrimSpace(os.Getenv("STORAGE_TYPE")))
	r2Bucket = strings.TrimSpace(os.Getenv("R2_BUCKET"))
	r2PublicBaseURL = strings.TrimRight(strings.TrimSpace(os.Getenv("R2_PUBLIC_BASE_URL")), "/")

	if storageType == "" && r2Bucket == "" {
		useR2Storage = false
		log.Println("File storage: local uploads directory")
		return nil
	}

	if storageType != "" && storageType != "r2" {
		useR2Storage = false
		log.Printf("File storage: local uploads directory (STORAGE_TYPE=%s)", storageType)
		return nil
	}

	accountID := strings.TrimSpace(os.Getenv("R2_ACCOUNT_ID"))
	accessKeyID := strings.TrimSpace(os.Getenv("R2_ACCESS_KEY_ID"))
	secretAccessKey := strings.TrimSpace(os.Getenv("R2_SECRET_ACCESS_KEY"))

	if accountID == "" || accessKeyID == "" || secretAccessKey == "" || r2Bucket == "" || r2PublicBaseURL == "" {
		return errors.New("R2配置不完整：需要 R2_ACCOUNT_ID、R2_ACCESS_KEY_ID、R2_SECRET_ACCESS_KEY、R2_BUCKET、R2_PUBLIC_BASE_URL")
	}

	endpoint := "https://" + accountID + ".r2.cloudflarestorage.com"

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				if service == s3.ServiceID {
					return aws.Endpoint{URL: endpoint, SigningRegion: "auto"}, nil
				}
				return aws.Endpoint{}, &aws.EndpointNotFoundError{}
			},
		)),
	)
	if err != nil {
		return err
	}

	r2Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	useR2Storage = true
	log.Printf("File storage: Cloudflare R2 bucket=%s public=%s", r2Bucket, r2PublicBaseURL)
	return nil
}

func isImageMime(mimeType string) bool {
	mimeType = strings.ToLower(strings.TrimSpace(mimeType))
	return strings.HasPrefix(mimeType, "image/")
}

func detectUploadMime(file multipartFile) string {
	buf := make([]byte, 512)
	n, _ := file.Read(buf)
	_, _ = file.Seek(0, io.SeekStart)
	if n > 0 {
		return http.DetectContentType(buf[:n])
	}
	return "application/octet-stream"
}

type multipartFile interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

func buildObjectKey(originalName string) string {
	now := time.Now()
	ext := strings.ToLower(filepath.Ext(originalName))
	if len(ext) > 16 {
		ext = ""
	}
	rnd, err := randomHex(16)
	if err != nil {
		rnd = fmt.Sprintf("%d", mrand.Int63())
	}
	return fmt.Sprintf("uploads/%04d/%02d/%02d/%s%s", now.Year(), int(now.Month()), now.Day(), rnd, ext)
}

func buildLocalFileName(originalName string) string {
	ext := strings.ToLower(filepath.Ext(originalName))
	if len(ext) > 16 {
		ext = ""
	}
	rnd, err := randomHex(16)
	if err != nil {
		rnd = fmt.Sprintf("%d", mrand.Int63())
	}
	return rnd + ext
}

func publicURLForObjectKey(key string) string {
	parts := strings.Split(key, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return r2PublicBaseURL + "/" + strings.Join(parts, "/")
}

func uploadToR2(ctx context.Context, key string, body io.Reader, size int64, mimeType string) error {
	_, err := r2Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(r2Bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(mimeType),
	})
	return err
}

func sanitizeUploadName(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	if name == "" || name == "." || name == string(filepath.Separator) {
		name = "file"
	}
	name = strings.ReplaceAll(name, "\x00", "")
	if len(name) > 200 {
		name = name[:200]
	}
	return name
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if _, ok := getInviteCodeFromRequest(r); !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	nick, ok := sessionNickFromRequest(r)
	if !ok {
		http.Error(w, "session required", http.StatusUnauthorized)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes+1024*1024)
	if err := r.ParseMultipartForm(maxUploadBytes); err != nil {
		http.Error(w, "file too large", http.StatusRequestEntityTooLarge)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "upload error", http.StatusBadRequest)
		return
	}
	defer file.Close()

	if header.Size > maxUploadBytes {
		http.Error(w, "file too large", http.StatusRequestEntityTooLarge)
		return
	}

	originalName := sanitizeUploadName(header.Filename)
	mimeType := strings.TrimSpace(header.Header.Get("Content-Type"))
	if mimeType == "" || mimeType == "application/octet-stream" {
		if mf, ok := file.(multipartFile); ok {
			mimeType = detectUploadMime(mf)
		}
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	var fileURL string
	var written int64

	if useR2Storage {
    mf, ok := file.(multipartFile)
		if !ok {
			http.Error(w, "upload stream error", http.StatusInternalServerError)
			return
		}
		_, _ = mf.Seek(0, io.SeekStart)

		storedKey := buildObjectKey(originalName)
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
		defer cancel()

		if err := uploadToR2(ctx, storedKey, mf, header.Size, mimeType); err != nil {
			log.Println("R2 upload error:", err)
			http.Error(w, "r2 upload error", http.StatusInternalServerError)
			return
		}
		written = header.Size
		fileURL = publicURLForObjectKey(storedKey)

  } else {
		if err := os.MkdirAll(uploadDir, 0o755); err != nil {
			http.Error(w, "mkdir error", http.StatusInternalServerError)
			return
		}

		storedName := buildLocalFileName(originalName)
		path := filepath.Join(uploadDir, storedName)

		out, err := os.Create(path)
		if err != nil {
			http.Error(w, "save error", http.StatusInternalServerError)
			return
		}
		defer out.Close()

		if mf, ok := file.(multipartFile); ok {
			_, _ = mf.Seek(0, io.SeekStart)
		}

		written, err = io.Copy(out, file)
		if err != nil {
			http.Error(w, "write error", http.StatusInternalServerError)
			return
		}
		fileURL = "/files/" + storedName
	}

	log.Printf("upload: nick=%s size=%d mime=%s name=%q", nick, written, mimeType, originalName)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"url":      fileURL,
		"name":     originalName,
		"size":     written,
		"mime":     mimeType,
		"is_image": isImageMime(mimeType),
	})
}

// ============================================================
// 静态 / 路由辅助
// ============================================================

func isHTTPSRequest(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func noDirList(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/") || r.URL.Path == "" {
			http.NotFound(w, r)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func setSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "same-origin")
	w.Header().Set("X-Frame-Options", "SAMEORIGIN")
}

func setIndexCSP(w http.ResponseWriter) {
	w.Header().Set("Content-Security-Policy",
		"default-src 'self'; "+
			"style-src 'self' 'unsafe-inline'; "+
			"script-src 'self' 'unsafe-inline'; "+
			"img-src 'self' data: blob: https:; "+
			"media-src 'self' blob: https:; "+
			"connect-src 'self' ws: wss:; "+
			"frame-ancestors 'self'; "+
			"base-uri 'self'")
}

func originAllowed(origin string, r *http.Request) bool {
	if origin == "" {
		return true // 非浏览器或同源请求
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	host := strings.ToLower(u.Host)
	if strings.EqualFold(host, strings.ToLower(r.Host)) {
		return true
	}
	for _, ao := range allowedOrigins {
		if ao == host {
			return true
		}
	}
	return false
}

// ============================================================
// main
// ============================================================

func main() {
	loadConfigFile()
	initSessionKey()
	initAllowedOrigins()

	serverPort = strings.TrimSpace(os.Getenv("PORT"))
	if serverPort == "" {
		serverPort = "8080"
	}
	listenAddr = strings.TrimSpace(os.Getenv("LISTEN_ADDR"))
	if listenAddr == "" {
		listenAddr = "127.0.0.1:" + serverPort
	}

	hub = initHub()

	if err := setupR2Storage(context.Background()); err != nil {
		log.Fatal(err)
	}

	if err := setupDatabase(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	importInviteCodesFromFile("./invite_codes.txt")

	if err := loadAllUsersToMemory(); err != nil {
		log.Println("load users warn:", err)
	}

	startSaveWorker()
	startReadStateFlusher()
	startUserListBroadcaster()
	go hub.run()

	mux := http.NewServeMux()

	if !useR2Storage {
		if err := os.MkdirAll(uploadDir, 0o755); err != nil {
			log.Fatal(err)
		}
		mux.Handle("/files/", http.StripPrefix("/files/", noDirList(http.FileServer(http.Dir(uploadDir)))))
	}

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if shuttingDown.Load() {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/upload", handleUpload)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		setSecurityHeaders(w)

		if _, ok := getInviteCodeFromRequest(r); !ok {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		setIndexCSP(w)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})

	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		setSecurityHeaders(w)

		if r.Method == http.MethodPost {
			ip := clientIP(r)
			rateKey := "login_invite|" + ip
			if isLoginBlocked(rateKey) {
				time.Sleep(800 * time.Millisecond)
				http.Redirect(w, r, "/login?error=1", http.StatusSeeOther)
				return
			}

			inviteCode := strings.TrimSpace(r.FormValue("invite_code"))
			if isInviteCodeValid(inviteCode) {
				clearLoginFailures(rateKey)
				http.SetCookie(w, &http.Cookie{
					Name:     "invite_code",
					Value:    inviteCode,
					Path:     "/",
					MaxAge:   86400 * 7,
					HttpOnly: true,
					Secure:   isHTTPSRequest(r),
					SameSite: http.SameSiteLaxMode,
				})
				http.Redirect(w, r, "/", http.StatusSeeOther)
				return
			}
			recordLoginFailure(rateKey)
			time.Sleep(400 * time.Millisecond)
			http.Redirect(w, r, "/login?error=1", http.StatusSeeOther)
			return
		}

		errorText := ""
		if r.URL.Query().Get("error") == "1" {
			errorText = `<p style="color:#ff7875;font-size:12px;margin:0 0 10px 0;">邀请码无效或已被禁用</p>`
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Login</title><style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;background:#1a1d24;color:#fff}form{background:#2a2d36;padding:30px;border-radius:15px;box-shadow:0 10px 30px rgba(0,0,0,0.5)}input{display:block;width:100%;margin:15px 0;padding:12px;border-radius:8px;border:none}button{width:100%;padding:12px;background:#6c63ff;color:#fff;border:none;border-radius:8px;cursor:pointer}</style></head><body><form method="POST"><h2>宇宙公司聊天室</h2>` + errorText + `<input type="text" name="invite_code" placeholder="请输入邀请码" required autofocus><button type="submit">进入聊天室</button></form></body></html>`))
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		inviteCode, ok := getInviteCodeFromRequest(r)
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		origin := r.Header.Get("Origin")
		if !originAllowed(origin, r) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}

		acceptOpts := &websocket.AcceptOptions{}
		if len(allowedOrigins) > 0 {
			acceptOpts.OriginPatterns = append([]string{r.Host}, allowedOrigins...)
		} else {
			acceptOpts.OriginPatterns = []string{r.Host}
		}

		conn, err := websocket.Accept(w, r, acceptOpts)
		if err != nil {
			return
		}

		ctx, cancel := context.WithCancel(r.Context())
		client := &Client{
			conn:           conn,
			inviteCode:     inviteCode,
			send:           make(chan Message, sendQueueSize),
			cancel:         cancel,
			lastReadMap:    make(map[string]int64),
			mentionReadMap: make(map[string]bool),
		}

		hub.register <- client
		ip := clientIP(r)
		go client.writePump(ctx)
		client.readPump(ctx, ip)
	})

	server := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}

	// 优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("shutting down...")
		shuttingDown.Store(true)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_ = server.Shutdown(ctx)

		flushAllReadStates()

		// 排空 saveChan
		close(hub.saveChan)
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer drainCancel()
		for {
			select {
			case <-drainCtx.Done():
				log.Println("save drain timeout")
				return
			default:
			}
			// 借助 worker 自然消耗：等队列长度归零
			if len(hub.saveChan) == 0 {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	log.Printf("Running on %s", listenAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	log.Println("server stopped")
}
