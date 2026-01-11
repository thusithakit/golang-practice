package main

import (
	"backend/internal/api"
	"backend/internal/engine"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func main() {
	log.Println("Starting System (Modular Engine)...")
	csvFile := "GO_Test.csv"
	if _, err := os.Stat(csvFile); os.IsNotExist(err) {
		log.Fatal("CSV not found")
	}

	store := engine.LoadColumnar(csvFile)

	log.Println("Aggregating...")
	t0 := time.Now()
	data := store.Aggregate()
	log.Printf("Aggregation Complete in %v", time.Since(t0))

	store = nil
	runtime.GC()

	e := echo.New()
	e.Use(middleware.CORS())
	e.Use(middleware.Logger())

	h := api.NewHandler(data)
	h.RegisterRoutes(e)

	log.Println("Server ready on port 8080")
	e.Logger.Fatal(e.Start(":8080"))
}
