package main

import (
    "encoding/json"
    "log"
    "net/http"
    "os"
    "sync"
    "time"

    "github.com/segmentio/kafka-go"
)

type Config struct {
    KafkaBrokers          []string
    MovieEventsTopic      string
    UserEventsTopic       string
    PaymentEventsTopic    string
    ServerPort            string
}

func (c *Config) TopicFor(path string) string {
    switch path {
    case "/api/events/movie":
        return c.MovieEventsTopic
    case "/api/events/user":
        return c.UserEventsTopic
    case "/api/events/payment":
        return c.PaymentEventsTopic
    default:
        return ""
    }
}

type KafkaProducer struct {
    writers map[string]*kafka.Writer
    mu      sync.RWMutex
}

func NewKafkaProducer(brokers []string) *KafkaProducer {
    writers := make(map[string]*kafka.Writer)
    topics := []string{"movie-events", "user-events", "payment-events"}
    for _, t := range topics {
        writers[t] = kafka.NewWriter(kafka.WriterConfig{
            Brokers:  brokers,
            Topic:    t,
            Balancer: &kafka.LeastBytes{},
        })
    }
    return &KafkaProducer{writers: writers}
}

func (kp *KafkaProducer) Close() {
    kp.mu.Lock()
    defer kp.mu.Unlock()
    for _, w := range kp.writers {
        w.Close()
    }
}

func (kp *KafkaProducer) Publish(topic string, data []byte) (partition int, offset int64, err error) {
    kp.mu.RLock()
    w, ok := kp.writers[topic]
    kp.mu.RUnlock()
    if !ok {
        return 0, 0, nil
    }
    msg := kafka.Message{Value: data}
    err = w.WriteMessages(nil, msg)
    return 0, 0, err
}

type Event struct {
    ID        string      `json:"id"`
    Type      string      `json:"type"`
    Timestamp string      `json:"timestamp"`
    Payload   interface{} `json:"payload"`
}

type MovieEvent struct {
    MovieID     int      `json:"movie_id"`
    Title       string   `json:"title"`
    Action      string   `json:"action"`
    UserID      int      `json:"user_id,omitempty"`
    Rating      float64  `json:"rating,omitempty"`
    Genres      []string `json:"genres,omitempty"`
    Description string   `json:"description,omitempty"`
}

type UserEvent struct {
    UserID    int    `json:"user_id"`
    Username  string `json:"username,omitempty"`
    Email     string `json:"email,omitempty"`
    Action    string `json:"action"`
    Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
    PaymentID  int     `json:"payment_id"`
    UserID     int     `json:"user_id"`
    Amount     float64 `json:"amount"`
    Status     string  `json:"status"`
    Timestamp  string  `json:"timestamp"`
    MethodType string  `json:"method_type,omitempty"`
}

type EventResponse struct {
    Status    string `json:"status"`
    Partition int    `json:"partition"`
    Offset    int64  `json:"offset"`
    Event     Event  `json:"event"`
}

type ErrorResponse struct {
    Error string `json:"error"`
}

type Handler struct {
    config   *Config
    producer *KafkaProducer
}

func NewHandler(cfg *Config, p *KafkaProducer) *Handler {
    return &Handler{config: cfg, producer: p}
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
    writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

func (h *Handler) movieEvent(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
        return
    }

    var ev MovieEvent
    if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
        writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid request body"})
        return
    }

    if ev.MovieID == 0 || ev.Title == "" || ev.Action == "" {
        writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "missing required fields: movie_id, title, action"})
        return
    }

    event := Event{
        ID:        "movie-" + itoa(ev.MovieID) + "-" + ev.Action,
        Type:      "movie",
        Timestamp: time.Now().UTC().Format(time.RFC3339),
        Payload:   ev,
    }

    topic := h.config.TopicFor("/api/events/movie")
    data, _ := json.Marshal(event)

    part, off, err := h.producer.Publish(topic, data)
    if err != nil {
        log.Printf("kafka publish error: %v", err)
        writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to publish event"})
        return
    }

    writeJSON(w, http.StatusCreated, EventResponse{
        Status:    "success",
        Partition: part,
        Offset:    off,
        Event:     event,
    })
}

func (h *Handler) userEvent(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
        return
    }

    var ev UserEvent
    if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
        writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid request body"})
        return
    }

    if ev.UserID == 0 || ev.Action == "" || ev.Timestamp == "" {
        writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "missing required fields: user_id, action, timestamp"})
        return
    }

    event := Event{
        ID:        "user-" + itoa(ev.UserID) + "-" + ev.Action,
        Type:      "user",
        Timestamp: ev.Timestamp,
        Payload:   ev,
    }

    topic := h.config.TopicFor("/api/events/user")
    data, _ := json.Marshal(event)

    part, off, err := h.producer.Publish(topic, data)
    if err != nil {
        log.Printf("kafka publish error: %v", err)
        writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to publish event"})
        return
    }

    writeJSON(w, http.StatusCreated, EventResponse{
        Status:    "success",
        Partition: part,
        Offset:    off,
        Event:     event,
    })
}

func (h *Handler) paymentEvent(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
        return
    }

    var ev PaymentEvent
    if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
        writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid request body"})
        return
    }

    if ev.PaymentID == 0 || ev.UserID == 0 || ev.Amount == 0 || ev.Status == "" || ev.Timestamp == "" {
        writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "missing required fields: payment_id, user_id, amount, status, timestamp"})
        return
    }

    event := Event{
        ID:        "payment-" + itoa(ev.PaymentID) + "-" + ev.Status,
        Type:      "payment",
        Timestamp: ev.Timestamp,
        Payload:   ev,
    }

    topic := h.config.TopicFor("/api/events/payment")
    data, _ := json.Marshal(event)

    part, off, err := h.producer.Publish(topic, data)
    if err != nil {
        log.Printf("kafka publish error: %v", err)
        writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to publish event"})
        return
    }

    writeJSON(w, http.StatusCreated, EventResponse{
        Status:    "success",
        Partition: part,
        Offset:    off,
        Event:     event,
    })
}

func itoa(n int) string {
    if n == 0 {
        return "0"
    }
    s := ""
    for n > 0 {
        s = string(rune('0'+n%10)) + s
        n /= 10
    }
    return s
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    json.NewEncoder(w).Encode(v)
}

func main() {
    brokers := os.Getenv("KAFKA_BROKERS")
    if brokers == "" {
        brokers = "localhost:9092"
    }
    port := os.Getenv("PORT")
    if port == "" {
        port = "8082"
    }

    cfg := &Config{
        KafkaBrokers:       []string{brokers},
        MovieEventsTopic:   "movie-events",
        UserEventsTopic:    "user-events",
        PaymentEventsTopic: "payment-events",
        ServerPort:         port,
    }

    producer := NewKafkaProducer(cfg.KafkaBrokers)
    defer producer.Close()

    h := NewHandler(cfg, producer)

    http.HandleFunc("/api/events/health", h.health)
    http.HandleFunc("/api/events/movie", h.movieEvent)
    http.HandleFunc("/api/events/user", h.userEvent)
    http.HandleFunc("/api/events/payment", h.paymentEvent)

    log.Printf("Events service listening on :%s", port)
    log.Fatal(http.ListenAndServe(":"+port, nil))
}
