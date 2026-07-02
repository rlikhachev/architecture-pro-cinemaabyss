package main

import (
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "strconv"
)

type ProxyController struct {
    monolithURL    string
    moviesURL      string
    eventsURL      string
    migrationPercent int
}

type HealthResponse struct {
    Status           string `json:"status"`
    RouteToMovies     bool   `json:"route_to_movies,omitempty"`
    MigrationPercent int    `json:"migration_percent"`
}

func NewProxyController() *ProxyController {
    percentage := getEnvInt("MOVIES_MIGRATION_PERCENT", 50)
    return &ProxyController{
        monolithURL:    os.Getenv("MONOLITH_URL"),
        moviesURL:      os.Getenv("MOVIES_SERVICE_URL"),
        eventsURL:      os.Getenv("EVENTS_SERVICE_URL"),
        migrationPercent: percentage,
    }
}

func hashString(s string) int {
    if s == "" {
        return 0
    }
    hash := 0
    for _, ch := range s {
        hash = (hash*31 + int(ch)) % 1000000007
    }
    return hash
}

func (pc *ProxyController) RouteRequest(r *http.Request) bool {
    if r.URL.Path == "/api/movies" || r.URL.Path == "/api/movies/" {
        hash := hashString(r.URL.Path + r.RemoteAddr)
        return hash%100 < pc.migrationPercent
    }
    if r.URL.Path == "/api/movies/health" {
        return true
    }
    if r.URL.Path == "/api/events/health" {
        return true
    }
    return false
}

func (pc *ProxyController) HandleRequest(w http.ResponseWriter, r *http.Request) {
    routeToMovies := pc.RouteRequest(r)

    if r.URL.Path == "/api/movies" || r.URL.Path == "/api/movies/" {
        if routeToMovies {
            pc.ProxyToMovies(w, r)
        } else {
            pc.ProxyToMonolith(w, r)
        }
        return
    }

    if r.URL.Path == "/health" {
        pc.HandleHealth(w, r)
        return
    }

    if routeToMovies {
        pc.ProxyToMovies(w, r)
    } else {
        pc.ProxyToMonolith(w, r)
    }
}

func (pc *ProxyController) ProxyToMovies(w http.ResponseWriter, r *http.Request) {
    if pc.moviesURL == "" {
        http.Error(w, "Movies service URL not configured", http.StatusBadGateway)
        return
    }
    pc.proxyRequest(w, r, pc.moviesURL)
}

func (pc *ProxyController) ProxyToMonolith(w http.ResponseWriter, r *http.Request) {
    if pc.monolithURL == "" {
        http.Error(w, "Monolith URL not configured", http.StatusBadGateway)
        return
    }
    pc.proxyRequest(w, r, pc.monolithURL)
}

func (pc *ProxyController) ProxyToEvents(w http.ResponseWriter, r *http.Request) {
    if pc.eventsURL == "" {
        http.Error(w, "Events service URL not configured", http.StatusBadGateway)
        return
    }
    pc.proxyRequest(w, r, pc.eventsURL)
}

func (pc *ProxyController) proxyRequest(w http.ResponseWriter, r *http.Request, targetURL string) {
    req, err := http.NewRequest(r.Method, targetURL+r.URL.RequestURI(), r.Body)
    if err != nil {
        http.Error(w, fmt.Sprintf("Failed to create request: %v", err), http.StatusInternalServerError)
        return
    }

    for key, values := range r.Header {
        for _, value := range values {
            req.Header.Add(key, value)
        }
    }

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        http.Error(w, fmt.Sprintf("Failed to proxy request: %v", err), http.StatusBadGateway)
        return
    }
    defer resp.Body.Close()

    for key, values := range resp.Header {
        for _, value := range values {
            w.Header().Add(key, value)
        }
    }

    w.WriteHeader(resp.StatusCode)
    _, err = io.Copy(w, resp.Body)
    if err != nil {
        log.Printf("Failed to proxy response body: %v", err)
    }
}

func (pc *ProxyController) HandleHealth(w http.ResponseWriter, r *http.Request) {
    health := HealthResponse{
        Status:           "healthy",
        MigrationPercent: pc.migrationPercent,
    }
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(health)
}

func getEnvInt(key string, defaultValue int) int {
    if value := os.Getenv(key); value != "" {
        if intValue, err := strconv.Atoi(value); err == nil {
            return intValue
        }
    }
    return defaultValue
}

func main() {
    controller := NewProxyController()
    http.HandleFunc("/", controller.HandleRequest)

    port := os.Getenv("PORT")
    if port == "" {
        port = "8000"
    }
    log.Printf("Starting proxy service on port %s", port)
    log.Fatal(http.ListenAndServe(":"+port, nil))
}