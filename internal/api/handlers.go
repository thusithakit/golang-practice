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
	api.GET("/revenue", h.GetRevenueByCountry)
	api.GET("/products/top", h.GetTopProducts)
	api.GET("/regions/top", h.GetTopRegions)
	api.GET("/sales/monthly", h.GetMonthlySales)
}

// --- HANDLERS ---
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

func (h *Handler) GetRevenueByCountry(c echo.Context) error {
	stats := h.data.CountryStats
	total := len(stats)
	limit, offset := getPaginationParams(c, total)

	if offset >= total {
		return c.JSON(http.StatusOK, []models.CountryStat{})
	}

	end := offset + limit
	if end > total {
		end = total
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"data":   stats[offset:end],
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// returns Top 20 products
func (h *Handler) GetTopProducts(c echo.Context) error {
	data := h.data.TopProducts
	limit, _ := getPaginationParams(c, len(data))

	if limit < len(data) {
		return c.JSON(http.StatusOK, data[:limit])
	}
	return c.JSON(http.StatusOK, data)
}

// returns Top 30 regions
func (h *Handler) GetTopRegions(c echo.Context) error {
	data := h.data.TopRegions
	limit, _ := getPaginationParams(c, len(data))

	if limit < len(data) {
		return c.JSON(http.StatusOK, data[:limit])
	}
	return c.JSON(http.StatusOK, data)
}

// monthly sales
func (h *Handler) GetMonthlySales(c echo.Context) error {
	return c.JSON(http.StatusOK, h.data.MonthlySales)
}
