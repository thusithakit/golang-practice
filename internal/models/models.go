package models

type DashboardData struct {
	CountryStats []CountryStat `json:"country_stats"`
	TopProducts  []TopItem     `json:"top_products"`
	TopRegions   []TopItem     `json:"top_regions"`

	// Changed: Key is Year (e.g., "2021"), Value is Jan-Dec data
	MonthlySales map[string][]MonthlyItem `json:"monthly_sales"`
}

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
