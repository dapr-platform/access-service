package main

import (
	"access-service/api"
	_ "access-service/docs"
	"access-service/eventsub"
	"github.com/dapr-platform/common"
	daprd "github.com/dapr/go-sdk/service/http"
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	httpSwagger "github.com/swaggo/http-swagger"
	"log"
	"net/http"
	"os"
	"strconv"
)

var (
	PORT = 80
)

func init() {

	if val := os.Getenv("LISTEN_PORT"); val != "" {
		PORT, _ = strconv.Atoi(val)
	}
	common.Logger.Debug("use PORT ", PORT)
}

// @title access-service API
// @version 1.0
// @description access-service API
// @BasePath /swagger/access-service
func main() {
	mux := chi.NewRouter()
	api.InitRoute(mux)
	mux.Handle("/metrics", promhttp.Handler())

	mux.Handle("/swagger*", httpSwagger.WrapHandler)

	s := daprd.NewServiceWithMux(":"+strconv.Itoa(PORT), mux)
	eventsub.Sub(s)
	log.Println("server start")
	if err := s.Start(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("error: %v", err)
	}
}
