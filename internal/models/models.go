package models

type DashboardData struct {
	// 1. The requested array for the first API
	CountryStats []CountryStat `json:"country_stats"`

	// Other endpoints
	TopProducts  []TopItem     `json:"top_products"`
	TopRegions   []TopItem     `json:"top_regions"`
	MonthlySales []MonthlyItem `json:"monthly_sales"`
}

// Matches: "country-name, total revenue and number of transactions"
type CountryStat struct {
	Country      string  `json:"country"`
	Revenue      float64 `json:"revenue"`
	Transactions int     `json:"transactions"`
}

type TopItem struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Extra int     `json:"extra,omitempty"`
}

type MonthlyItem struct {
	Month  string  `json:"month"`
	Volume float64 `json:"sales"`
}
