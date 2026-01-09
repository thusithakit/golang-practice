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
	e := echo.New()
	e.Use(middleware.CORS())
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	h := api.NewHandler(nil)
	h.RegisterRoutes(e)

	go func() {
		log.Println("BACKGROUND: Starting ETL Pipeline...")
		t0 := time.Now()

		store := engine.LoadColumnar("GO_Test.csv")
		dashboardData := store.Aggregate()
		h.SetData(dashboardData)

		log.Printf("BACKGROUND: ETL Complete in %v. API is fully ready.", time.Since(t0))
	}()

	log.Println("Server ready on port 8080 (Data loading in background...)")
	e.Logger.Fatal(e.Start(":8080"))
}
