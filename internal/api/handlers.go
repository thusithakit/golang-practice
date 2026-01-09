package api

import (
	"backend/internal/models"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
)

type Handler struct {
	data *models.DashboardData
}

// NewHandler injects the pre-calculated data into the API layer
func NewHandler(data *models.DashboardData) *Handler {
	return &Handler{data: data}
}

// RegisterRoutes maps the endpoints to functions
func (h *Handler) RegisterRoutes(e *echo.Echo) {
	api := e.Group("/api")

	api.GET("/revenue", h.GetRevenueByCountry)   // Q1: Country Stats
	api.GET("/products/top", h.GetTopProducts)   // Q2: Top 20 Products
	api.GET("/regions/top", h.GetTopRegions)     // Q3: Top Regions
	api.GET("/sales/monthly", h.GetMonthlySales) // Q4: Monthly Trend
}

// --- HANDLERS ---

// GetRevenueByCountry returns the list of countries sorted by revenue.
// Supports query params: ?page=1&limit=50
func (h *Handler) GetRevenueByCountry(c echo.Context) error {
	stats := h.data.CountryStats

	// Simple Pagination Logic
	page, _ := strconv.Atoi(c.QueryParam("page"))
	limit, _ := strconv.Atoi(c.QueryParam("limit"))

	if page < 1 {
		page = 1
	}
	if limit < 1 {
		// Return all if no limit specified (fastest for small lists ~200 items)
		return c.JSON(http.StatusOK, stats)
	}

	start := (page - 1) * limit
	if start >= len(stats) {
		return c.JSON(http.StatusOK, []models.CountryStat{})
	}
	end := start + limit
	if end > len(stats) {
		end = len(stats)
	}

	return c.JSON(http.StatusOK, stats[start:end])
}

// GetTopProducts returns the top 20 products
func (h *Handler) GetTopProducts(c echo.Context) error {
	return c.JSON(http.StatusOK, h.data.TopProducts)
}

// GetTopRegions returns the top performing regions
func (h *Handler) GetTopRegions(c echo.Context) error {
	return c.JSON(http.StatusOK, h.data.TopRegions)
}

// GetMonthlySales returns sales volume by month (Jan -> Dec)
func (h *Handler) GetMonthlySales(c echo.Context) error {
	return c.JSON(http.StatusOK, h.data.MonthlySales)
}
