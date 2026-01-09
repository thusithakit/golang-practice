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

func NewHandler(data *models.DashboardData) *Handler {
	return &Handler{data: data}
}

func (h *Handler) RegisterRoutes(e *echo.Echo) {
	api := e.Group("/api")
	api.GET("/revenue", h.GetRevenueByCountry)   // ?limit=10&offset=0
	api.GET("/products/top", h.GetTopProducts)   // ?limit=5
	api.GET("/regions/top", h.GetTopRegions)     // ?limit=5
	api.GET("/sales/monthly", h.GetMonthlySales) // No params needed
}

// --- HANDLERS ---

// Helper to parse limit/offset safely
func getPaginationParams(c echo.Context, defaultLimit int) (int, int) {
	limit, err := strconv.Atoi(c.QueryParam("limit"))
	if err != nil || limit <= 0 {
		limit = defaultLimit
	}
	offset, err := strconv.Atoi(c.QueryParam("offset"))
	if err != nil || offset < 0 {
		offset = 0
	}
	return limit, offset
}

// GetRevenueByCountry returns country stats with pagination.
// Default: All items (limit=1000)
func (h *Handler) GetRevenueByCountry(c echo.Context) error {
	stats := h.data.CountryStats
	total := len(stats)

	// Default to returning everything if no params provided,
	// but allow slicing if requested.
	limit, offset := getPaginationParams(c, total)

	if offset >= total {
		return c.JSON(http.StatusOK, []models.CountryStat{})
	}

	end := offset + limit
	if end > total {
		end = total
	}

	// Return sliced data
	return c.JSON(http.StatusOK, map[string]interface{}{
		"data":   stats[offset:end],
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// GetTopProducts returns Top 20 (calculated) but allows frontend to request fewer (e.g., Top 5)
func (h *Handler) GetTopProducts(c echo.Context) error {
	data := h.data.TopProducts
	limit, _ := getPaginationParams(c, len(data))

	if limit < len(data) {
		return c.JSON(http.StatusOK, data[:limit])
	}
	return c.JSON(http.StatusOK, data)
}

// GetTopRegions returns Top 30 (calculated) but allows frontend to request fewer
func (h *Handler) GetTopRegions(c echo.Context) error {
	data := h.data.TopRegions
	limit, _ := getPaginationParams(c, len(data))

	if limit < len(data) {
		return c.JSON(http.StatusOK, data[:limit])
	}
	return c.JSON(http.StatusOK, data)
}

// GetMonthlySales is fixed at 12 months, no pagination needed.
func (h *Handler) GetMonthlySales(c echo.Context) error {
	return c.JSON(http.StatusOK, h.data.MonthlySales)
}
