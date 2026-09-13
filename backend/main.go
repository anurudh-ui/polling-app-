package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/crypto/bcrypt"
)

type User struct {
	ID        primitive.ObjectID `bson:"_id" json:"id"`
	Name      string             `bson:"name" json:"name"`
	Email     string             `bson:"email" json:"email"`
	Password  string             `bson:"password" json:"-"`
	CreatedAt time.Time          `bson:"createdAt" json:"createdAt"`
}
type Option struct {
	ID    string `bson:"id" json:"id"`
	Text  string `bson:"text" json:"text"`
	Votes int    `bson:"votes" json:"votes"`
}
type Poll struct {
	ID          primitive.ObjectID `bson:"_id" json:"id"`
	Slug        string             `bson:"slug" json:"slug"`
	Question    string             `bson:"question" json:"question"`
	Options     []Option           `bson:"options" json:"options"`
	CreatorID   primitive.ObjectID `bson:"creatorId" json:"creatorId"`
	CreatorName string             `bson:"creatorName" json:"creatorName"`
	CreatedAt   time.Time          `bson:"createdAt" json:"createdAt"`
	ExpiresAt   *time.Time         `bson:"expiresAt,omitempty" json:"expiresAt,omitempty"`
	TotalVotes  int                `bson:"totalVotes" json:"totalVotes"`
	Multiple    bool               `bson:"multiple" json:"multiple"`
}
type Vote struct {
	ID        primitive.ObjectID `bson:"_id"`
	PollID    primitive.ObjectID `bson:"pollId"`
	VoterID   string             `bson:"voterId"`
	OptionIDs []string           `bson:"optionIds"`
	CreatedAt time.Time          `bson:"createdAt"`
}
type Claims struct {
	UserID string `json:"uid"`
	jwt.RegisteredClaims
}
type Server struct {
	polls, users, votes *mongo.Collection
	rdb                 *redis.Client
	upgrader            websocket.Upgrader
	rooms               map[string]map[*websocket.Conn]bool
	mu                  sync.Mutex
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func randomSlug() string {
	b := make([]byte, 5)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func token(uid string) string {
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{UserID: uid, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(7 * 24 * time.Hour))}})
	s, _ := t.SignedString([]byte(env("JWT_SECRET", "change-me")))
	return s
}
func auth(s *Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.GetHeader("Authorization")
		if !strings.HasPrefix(h, "Bearer ") {
			c.AbortWithStatusJSON(401, gin.H{"error": "authentication required"})
			return
		}
		t, e := jwt.ParseWithClaims(strings.TrimPrefix(h, "Bearer "), &Claims{}, func(t *jwt.Token) (any, error) { return []byte(env("JWT_SECRET", "change-me")), nil })
		if e != nil || !t.Valid {
			c.AbortWithStatusJSON(401, gin.H{"error": "invalid token"})
			return
		}
		cl := t.Claims.(*Claims)
		id, e := primitive.ObjectIDFromHex(cl.UserID)
		if e != nil {
			c.AbortWithStatus(401)
			return
		}
		c.Set("uid", id)
		c.Next()
	}
}
func uid(c *gin.Context) primitive.ObjectID { return c.MustGet("uid").(primitive.ObjectID) }
func voterID(c *gin.Context) string {
	if v, e := c.Cookie("pollify_voter"); e == nil && v != "" {
		return v
	}
	b := make([]byte, 16)
	rand.Read(b)
	v := hex.EncodeToString(b)
	http.SetCookie(c.Writer, &http.Cookie{Name: "pollify_voter", Value: v, Path: "/", MaxAge: 31536000, HttpOnly: true, SameSite: http.SameSiteLaxMode})
	return v
}
func validEmail(v string) bool {
	return regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`).MatchString(v)
}
func (s *Server) broadcast(slug string, p Poll) {
	payload, _ := json.Marshal(p)
	s.rdb.Publish(context.Background(), "poll:"+slug, payload)
}
func (s *Server) startRealtime() {
	pub := s.rdb.PSubscribe(context.Background(), "poll:*")
	go func() {
		for {
			msg, e := pub.ReceiveMessage(context.Background())
			if e != nil {
				time.Sleep(time.Second)
				continue
			}
			var p Poll
			if json.Unmarshal([]byte(msg.Payload), &p) != nil {
				continue
			}
			data, _ := json.Marshal(gin.H{"type": "poll_update", "poll": p})
			slug := strings.TrimPrefix(msg.Channel, "poll:")
			s.mu.Lock()
			for c := range s.rooms[slug] {
				if e := c.WriteMessage(websocket.TextMessage, data); e != nil {
					delete(s.rooms[slug], c)
					c.Close()
				}
			}
			s.mu.Unlock()
		}
	}()
}

func main() {
	ctx := context.Background()
	mc, e := mongo.Connect(ctx, options.Client().ApplyURI(env("MONGO_URI", "mongodb://localhost:27017")))
	if e != nil {
		log.Fatal(e)
	}
	if e = mc.Ping(ctx, nil); e != nil {
		log.Fatal(e)
	}
	db := mc.Database(env("MONGO_DB", "pollify"))
	s := &Server{users: db.Collection("users"), polls: db.Collection("polls"), votes: db.Collection("votes"), rdb: redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")}), upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}, rooms: map[string]map[*websocket.Conn]bool{}}
	_, _ = s.users.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "email", Value: 1}}, Options: options.Index().SetUnique(true)})
	_, _ = s.polls.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "slug", Value: 1}}, Options: options.Index().SetUnique(true)})
	_, _ = s.votes.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "pollId", Value: 1}, {Key: "voterId", Value: 1}}, Options: options.Index().SetUnique(true)})
	r := gin.Default()
	r.Use(func(c *gin.Context) {
		origin := env("FRONTEND_URL", "http://localhost:5173")
		c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) })
	r.POST("/api/auth/signup", s.signup)
	r.POST("/api/auth/login", s.login)
	r.GET("/api/polls", s.listPolls)
	r.GET("/api/polls/:slug", s.getPoll)
	r.GET("/api/polls/:slug/ws", s.ws)
	r.POST("/api/polls/:slug/vote", s.vote)
	a := r.Group("/api", auth(s))
	a.POST("/polls", s.createPoll)
	a.GET("/me/polls", s.myPolls)
	s.startRealtime()
	log.Println("Pollify API listening on :8080")
	log.Fatal(r.Run(":8080"))
}

func (s *Server) signup(c *gin.Context) {
	var in struct{ Name, Email, Password string }
	if c.ShouldBindJSON(&in) != nil || len(strings.TrimSpace(in.Name)) < 2 || !validEmail(strings.TrimSpace(in.Email)) || len(in.Password) < 6 {
		c.JSON(400, gin.H{"error": "name, valid email and password of 6+ chars required"})
		return
	}
	email := strings.ToLower(strings.TrimSpace(in.Email))
	h, _ := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
	u := User{ID: primitive.NewObjectID(), Name: strings.TrimSpace(in.Name), Email: email, Password: string(h), CreatedAt: time.Now()}
	if _, e := s.users.InsertOne(c, u); e != nil {
		c.JSON(409, gin.H{"error": "email already registered"})
		return
	}
	c.JSON(201, gin.H{"token": token(u.ID.Hex()), "user": u})
}
func (s *Server) login(c *gin.Context) {
	var in struct{ Email, Password string }
	if c.ShouldBindJSON(&in) != nil {
		c.JSON(400, gin.H{"error": "invalid request"})
		return
	}
	var u User
	if e := s.users.FindOne(c, bson.M{"email": strings.ToLower(strings.TrimSpace(in.Email))}).Decode(&u); e != nil || bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(in.Password)) != nil {
		c.JSON(401, gin.H{"error": "invalid credentials"})
		return
	}
	c.JSON(200, gin.H{"token": token(u.ID.Hex()), "user": u})
}
func (s *Server) createPoll(c *gin.Context) {
	var in struct {
		Question  string     `json:"question"`
		Options   []string   `json:"options"`
		Multiple  bool       `json:"multiple"`
		ExpiresAt *time.Time `json:"expiresAt"`
	}
	if c.ShouldBindJSON(&in) != nil {
		c.JSON(400, gin.H{"error": "invalid request"})
		return
	}
	q := strings.TrimSpace(in.Question)
	if len(q) < 3 || len(q) > 240 || len(in.Options) < 2 || len(in.Options) > 10 {
		c.JSON(400, gin.H{"error": "question must be 3-240 chars and 2-10 options"})
		return
	}
	opts := make([]Option, 0, len(in.Options))
	seen := map[string]bool{}
	for _, x := range in.Options {
		x = strings.TrimSpace(x)
		if x == "" || len(x) > 100 {
			c.JSON(400, gin.H{"error": "options cannot be empty or over 100 chars"})
			return
		}
		key := strings.ToLower(x)
		if seen[key] {
			c.JSON(400, gin.H{"error": "options must be unique"})
			return
		}
		seen[key] = true
		opts = append(opts, Option{ID: primitive.NewObjectID().Hex(), Text: x})
	}
	if in.ExpiresAt != nil && in.ExpiresAt.Before(time.Now()) {
		c.JSON(400, gin.H{"error": "end time must be in the future"})
		return
	}
	var u User
	_ = s.users.FindOne(c, bson.M{"_id": uid(c)}).Decode(&u)
	p := Poll{ID: primitive.NewObjectID(), Slug: randomSlug(), Question: q, Options: opts, CreatorID: uid(c), CreatorName: u.Name, CreatedAt: time.Now(), ExpiresAt: in.ExpiresAt, Multiple: in.Multiple}
	if _, e := s.polls.InsertOne(c, p); e != nil {
		c.JSON(500, gin.H{"error": "could not create poll"})
		return
	}
	c.JSON(201, p)
}
func (s *Server) listPolls(c *gin.Context) {
	limit := int64(12)
	cur, e := s.polls.Find(c, bson.M{}, options.Find().SetSort(bson.D{{Key: "createdAt", Value: -1}}).SetLimit(limit))
	if e != nil {
		c.JSON(500, gin.H{"error": "failed to load polls"})
		return
	}
	defer cur.Close(c)
	var out []Poll
	if e = cur.All(c, &out); e != nil {
		c.JSON(500, gin.H{"error": "failed to load polls"})
		return
	}
	c.JSON(200, out)
}
func (s *Server) getPoll(c *gin.Context) {
	var p Poll
	if e := s.polls.FindOne(c, bson.M{"slug": c.Param("slug")}).Decode(&p); e != nil {
		c.JSON(404, gin.H{"error": "poll not found"})
		return
	}
	c.JSON(200, p)
}
func (s *Server) vote(c *gin.Context) {
	var in struct {
		OptionIDs []string `json:"optionIds"`
	}
	if c.ShouldBindJSON(&in) != nil || len(in.OptionIDs) == 0 || len(in.OptionIDs) > 10 {
		c.JSON(400, gin.H{"error": "choose at least one option"})
		return
	}
	var p Poll
	if e := s.polls.FindOne(c, bson.M{"slug": c.Param("slug")}).Decode(&p); e != nil {
		c.JSON(404, gin.H{"error": "poll not found"})
		return
	}
	if p.ExpiresAt != nil && time.Now().After(*p.ExpiresAt) {
		c.JSON(410, gin.H{"error": "poll has ended"})
		return
	}
	if !p.Multiple && len(in.OptionIDs) != 1 {
		c.JSON(400, gin.H{"error": "choose one option"})
		return
	}
	allowed := map[string]bool{}
	for _, o := range p.Options {
		allowed[o.ID] = true
	}
	seen := map[string]bool{}
	for _, id := range in.OptionIDs {
		if !allowed[id] || seen[id] {
			c.JSON(400, gin.H{"error": "invalid or duplicate option"})
			return
		}
		seen[id] = true
	}
	voter := voterID(c)
	v := Vote{ID: primitive.NewObjectID(), PollID: p.ID, VoterID: voter, OptionIDs: in.OptionIDs, CreatedAt: time.Now()}
	if _, e := s.votes.InsertOne(c, v); e != nil {
		if mongo.IsDuplicateKeyError(e) {
			c.JSON(409, gin.H{"error": "you have already voted on this poll"})
			return
		}
		c.JSON(500, gin.H{"error": "vote could not be recorded"})
		return
	}
	for _, id := range in.OptionIDs {
		_, e := s.polls.UpdateOne(c, bson.M{"_id": p.ID}, bson.M{"$inc": bson.M{"options.$[o].votes": 1}}, options.Update().SetArrayFilters(options.ArrayFilters{Filters: []any{bson.M{"o.id": id}}}))
		if e != nil {
			c.JSON(500, gin.H{"error": "vote update failed"})
			return
		}
	}
	_, _ = s.polls.UpdateOne(c, bson.M{"_id": p.ID}, bson.M{"$inc": bson.M{"totalVotes": 1}})
	s.rdb.Incr(c, "poll:"+p.Slug+":votes")
	_ = s.polls.FindOne(c, bson.M{"_id": p.ID}).Decode(&p)
	s.broadcast(p.Slug, p)
	c.JSON(200, p)
}
func (s *Server) myPolls(c *gin.Context) {
	cur, e := s.polls.Find(c, bson.M{"creatorId": uid(c)}, options.Find().SetSort(bson.D{{Key: "createdAt", Value: -1}}).SetLimit(30))
	if e != nil {
		c.JSON(500, gin.H{"error": "failed"})
		return
	}
	defer cur.Close(c)
	var out []Poll
	if e = cur.All(c, &out); e != nil {
		c.JSON(500, gin.H{"error": "failed"})
		return
	}
	c.JSON(200, out)
}
func (s *Server) ws(c *gin.Context) {
	slug := c.Param("slug")
	conn, e := s.upgrader.Upgrade(c.Writer, c.Request, nil)
	if e != nil {
		return
	}
	s.mu.Lock()
	if s.rooms[slug] == nil {
		s.rooms[slug] = map[*websocket.Conn]bool{}
	}
	s.rooms[slug][conn] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.rooms[slug], conn)
		if len(s.rooms[slug]) == 0 {
			delete(s.rooms, slug)
		}
		s.mu.Unlock()
		conn.Close()
	}()
	for {
		if _, _, e := conn.ReadMessage(); e != nil {
			return
		}
	}
}
