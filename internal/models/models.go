package models

type DashboardData struct {
	CountryStats []CountryStat            `json:"country_stats"`
	TopProducts  []TopItem                `json:"top_products"`
	TopRegions   []TopRegion              `json:"top_regions"`
	MonthlySales map[string][]MonthlyItem `json:"monthly_sales"`
}

type CountryStat struct {
	Country      string  `json:"country"`
	Revenue      float64 `json:"revenue"`
	Transactions int     `json:"transactions"`
}

type TopItem struct {
	Name  string  `json:"product_name"`
	Value float64 `json:"items_sold"`
	Extra int     `json:"available,omitempty"`
}

type TopRegion struct {
	Name    string  `json:"region"`
	Revenue float64 `json:"revenue"`
}

type MonthlyItem struct {
	Month  string  `json:"month"`
	Volume float64 `json:"sales"`
}
