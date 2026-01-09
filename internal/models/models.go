package models

type DashboardData struct {
	TopProducts  []TopItem     `json:"top_products"`
	TopRegions   []TopItem     `json:"top_regions"`
	MonthlySales []MonthlyItem `json:"monthly_sales"`
	CountryTable []CountryRow  `json:"country_table"`
}

type TopItem struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Extra int     `json:"extra,omitempty"`
}

type MonthlyItem struct {
	Month  string  `json:"month"`
	Volume float64 `json:"volume"`
}

type CountryRow struct {
	Country      string  `json:"country"`
	Product      string  `json:"product"`
	Revenue      float64 `json:"revenue"`
	Transactions int     `json:"transactions"`
}
