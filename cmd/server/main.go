package main

import (
	"backend/internal/api"
	"backend/internal/engine"
	"log"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func main() {
	// 1. Initialize Echo (Starts Instantly)
	e := echo.New()
	e.Use(middleware.CORS())
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	// 2. Initialize Handler with NIL data
	// The API is now "live" but will return 503 (Loading) if hit
	h := api.NewHandler(nil)
	h.RegisterRoutes(e)

	// 3. Launch ETL in Background (The "Async" Trick)
	go func() {
		log.Println("BACKGROUND: Starting ETL Pipeline...")
		t0 := time.Now()

		// Run the Heavy Lifting
		// (Ensure internal/engine is using the code we wrote previously)
		store := engine.LoadColumnar("GO_Test.csv")
		dashboardData := store.Aggregate()

		// Update the live API with the fresh data
		h.SetData(dashboardData)

		log.Printf("BACKGROUND: ETL Complete in %v. API is fully ready.", time.Since(t0))
	}()

	// 4. Start Server (This happens immediately, <2ms)
	log.Println("Server ready on port 8080 (Data loading in background...)")
	e.Logger.Fatal(e.Start(":8080"))
}
