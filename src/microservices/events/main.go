package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "os"
    "time"

    "github.com/segmentio/kafka-go"
)

type Config struct {
    Port          string
    KafkaBrokers  string
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
    Status    string      `json:"status"`
    Partition int         `json:"partition"`
    Offset    int64       `json:"offset"`
    Event     Event       `json:"event"`
}

type ErrorResponse struct {
    Error string `json:"error"`
}

type KafkaWriter struct {
    writer *kafka.Writer
    topic  string
}

func NewKafkaWriter(brokers string, topic string) *KafkaWriter {
    if brokers == "" {
        return nil
    }
    return &KafkaWriter{
        topic: topic,
        writer: &kafka.Writer{
            Addr:     kafka.TCP(brokers),
            Topic:    topic,
            Balancer: &kafka.LeastBytes{},
        },
    }
}

func (w *KafkaWriter) Write(msg []byte) error {
    if w == nil || w.writer == nil {
        return nil
    }
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    err := w.writer.WriteMessages(ctx, kafka.Message{Value: msg})
    if err != nil {
        log.Printf("WRITE ERROR [%s]: %v", w.topic, err)
    } else {
        log.Printf("WRITE OK [%s]: %s", w.topic, string(msg))
    }
    return err
}

func (w *KafkaWriter) Close() {
    if w != nil && w.writer != nil {
        w.writer.Close()
    }
}

type KafkaReader struct {
    reader *kafka.Reader
    topic  string
}

func NewKafkaReader(brokers string, topic string) *KafkaReader {
    if brokers == "" {
        return nil
    }
    return &KafkaReader{
        topic: topic,
        reader: kafka.NewReader(kafka.ReaderConfig{
            Brokers:  []string{brokers},
            Topic:    topic,
            GroupID:  "events-service-group",
            MinBytes: 10,
            MaxBytes: 10e6,
            MaxWait:  1 * time.Second,
        }),
    }
}

func (r *KafkaReader) Start(ctx context.Context) {
    if r == nil || r.reader == nil {
        return
    }
    go func() {
        for {
            msg, err := r.reader.ReadMessage(ctx)
            if err != nil {
                log.Printf("READ ERROR [%s]: %v", r.topic, err)
                return
            }
            log.Printf("READ OK [%s] partition=%d offset=%d key=%s value=%s", r.topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
        }
    }()
}

func (r *KafkaReader) Close() {
    if r != nil && r.reader != nil {
        r.reader.Close()
    }
}

type Handler struct {
    movieWriter   *KafkaWriter
    userWriter    *KafkaWriter
    paymentWriter *KafkaWriter
    movieReader   *KafkaReader
    userReader    *KafkaReader
    paymentReader *KafkaReader
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
    writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

func (h *Handler) publish(topic string, data []byte) {
    var w *KafkaWriter
    switch topic {
    case "movie-events":
        w = h.movieWriter
    case "user-events":
        w = h.userWriter
    case "payment-events":
        w = h.paymentWriter
    default:
        return
    }
    w.Write(data)
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

    event := Event{
        ID:        evid("movie", ev.MovieID, ev.Action),
        Type:      "movie",
        Timestamp: time.Now().UTC().Format(time.RFC3339),
        Payload:   ev,
    }

    data, _ := json.Marshal(event)
    go h.publish("movie-events", data)

    writeJSON(w, http.StatusCreated, EventResponse{
        Status: "success",
        Event:  event,
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

    event := Event{
        ID:        evid("user", ev.UserID, ev.Action),
        Type:      "user",
        Timestamp: ev.Timestamp,
        Payload:   ev,
    }

    data, _ := json.Marshal(event)
    go h.publish("user-events", data)

    writeJSON(w, http.StatusCreated, EventResponse{
        Status: "success",
        Event:  event,
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

    event := Event{
        ID:        evid("payment", ev.PaymentID, ev.Status),
        Type:      "payment",
        Timestamp: ev.Timestamp,
        Payload:   ev,
    }

    data, _ := json.Marshal(event)
    go h.publish("payment-events", data)

    writeJSON(w, http.StatusCreated, EventResponse{
        Status: "success",
        Event:  event,
    })
}

func (h *Handler) triggerEvent(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
        return
    }

    now := time.Now().UTC().Format(time.RFC3339)
    results := []map[string]interface{}{}

    movieEv := MovieEvent{
        MovieID: 1, Title: "Triggered Movie", Action: "viewed",
        UserID: 1, Rating: 8.5, Genres: []string{"Drama"}, Description: "Triggered event",
    }
    movieEvent := Event{
        ID: evid("movie", movieEv.MovieID, movieEv.Action),
        Type: "movie", Timestamp: now, Payload: movieEv,
    }
    md, _ := json.Marshal(movieEvent)
    go h.publish("movie-events", md)
    results = append(results, map[string]interface{}{"type": "movie", "event": movieEvent})

    userEv := UserEvent{
        UserID: 42, Username: "triggered_user", Email: "trigger@test.com",
        Action: "created", Timestamp: now,
    }
    userEvent := Event{
        ID: evid("user", userEv.UserID, userEv.Action),
        Type: "user", Timestamp: now, Payload: userEv,
    }
    ud, _ := json.Marshal(userEvent)
    go h.publish("user-events", ud)
    results = append(results, map[string]interface{}{"type": "user", "event": userEvent})

    paymentEv := PaymentEvent{
        PaymentID: 100, UserID: 42, Amount: 29.99,
        Status: "completed", Timestamp: now, MethodType: "credit_card",
    }
    paymentEvent := Event{
        ID: evid("payment", paymentEv.PaymentID, paymentEv.Status),
        Type: "payment", Timestamp: now, Payload: paymentEv,
    }
    pd, _ := json.Marshal(paymentEvent)
    go h.publish("payment-events", pd)
    results = append(results, map[string]interface{}{"type": "payment", "event": paymentEvent})

    writeJSON(w, http.StatusCreated, map[string]interface{}{
        "status":  "triggered",
        "events":  results,
    })
}

func evid(prefix string, n int, suffix string) string {
    s := ""
    for n > 0 {
        s = string(rune('0'+n%10)) + s
        n /= 10
    }
    if s == "" {
        s = "0"
    }
    return prefix + "-" + s + "-" + suffix
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    json.NewEncoder(w).Encode(v)
}

func main() {
    port := os.Getenv("PORT")
    if port == "" {
        port = "8082"
    }

    brokers := os.Getenv("KAFKA_BROKERS")

    h := &Handler{
        movieWriter:   NewKafkaWriter(brokers, "movie-events"),
        userWriter:    NewKafkaWriter(brokers, "user-events"),
        paymentWriter: NewKafkaWriter(brokers, "payment-events"),
        movieReader:   NewKafkaReader(brokers, "movie-events"),
        userReader:    NewKafkaReader(brokers, "user-events"),
        paymentReader: NewKafkaReader(brokers, "payment-events"),
    }
    defer h.movieWriter.Close()
    defer h.userWriter.Close()
    defer h.paymentWriter.Close()
    defer h.movieReader.Close()
    defer h.userReader.Close()
    defer h.paymentReader.Close()

    ctx := context.Background()
    h.movieReader.Start(ctx)
    h.userReader.Start(ctx)
    h.paymentReader.Start(ctx)

    if brokers == "" {
        log.Println("KAFKA_BROKERS not set — events will be logged only")
    } else {
        log.Printf("connected to Kafka at %s", brokers)
    }

    http.HandleFunc("/api/events/health", h.health)
    http.HandleFunc("/api/events/movie", h.movieEvent)
    http.HandleFunc("/api/events/user", h.userEvent)
    http.HandleFunc("/api/events/payment", h.paymentEvent)
    http.HandleFunc("/api/events/trigger", h.triggerEvent)

    log.Printf("Events service listening on :%s", port)
    log.Fatal(http.ListenAndServe(":"+port, nil))
}
