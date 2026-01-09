package api

import (
	"backend/internal/models"
	"net/http"
	"strconv"
	"sync"

	"github.com/labstack/echo/v4"
)

// Handler holds the dashboard data with thread-safety mechanisms
type Handler struct {
	data *models.DashboardData
	mu   sync.RWMutex // Read/Write Mutex to prevent race conditions
}

// NewHandler initializes the handler (data can be nil initially)
func NewHandler(data *models.DashboardData) *Handler {
	return &Handler{data: data}
}

// SetData safely updates the dashboard data from the background ETL process
func (h *Handler) SetData(newData *models.DashboardData) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.data = newData
}

// isReady checks if data is loaded. If not, sends 503 response.
// Returns true if ready, false if loading.
func (h *Handler) isReady(c echo.Context) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.data == nil {
		c.JSON(http.StatusServiceUnavailable, map[string]string{
			"status":  "loading",
			"message": "System is warming up. Please try again in a few seconds.",
		})
		return false
	}
	return true
}

// RegisterRoutes connects the endpoints
func (h *Handler) RegisterRoutes(e *echo.Echo) {
	e.GET("/api/products/top", h.GetTopProducts)
	e.GET("/api/regions/top", h.GetTopRegions)
	e.GET("/api/sales/monthly", h.GetMonthlySales)
	e.GET("/api/revenue", h.GetRevenueTable)
}

// --- Handler Methods ---

func (h *Handler) GetTopProducts(c echo.Context) error {
	if !h.isReady(c) {
		return nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	return c.JSON(http.StatusOK, h.data.TopProducts)
}

func (h *Handler) GetTopRegions(c echo.Context) error {
	if !h.isReady(c) {
		return nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	return c.JSON(http.StatusOK, h.data.TopRegions)
}

func (h *Handler) GetMonthlySales(c echo.Context) error {
	if !h.isReady(c) {
		return nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	return c.JSON(http.StatusOK, h.data.MonthlySales)
}

func (h *Handler) GetRevenueTable(c echo.Context) error {
	if !h.isReady(c) {
		return nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	// Pagination Logic
	page, _ := strconv.Atoi(c.QueryParam("page"))
	if page < 1 {
		page = 1
	}
	limit := 50

	start := (page - 1) * limit
	totalRows := len(h.data.CountryTable)

	if start >= totalRows {
		return c.JSON(http.StatusOK, []models.CountryRow{})
	}

	end := start + limit
	if end > totalRows {
		end = totalRows
	}

	return c.JSON(http.StatusOK, h.data.CountryTable[start:end])
}
