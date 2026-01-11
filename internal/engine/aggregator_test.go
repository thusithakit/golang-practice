package engine

import (
	"testing"
)

func TestAggregate(t *testing.T) {
	// 1. Setup Mock Data (ColumnStore)
	// Scenario:
	// Row 0: Germany, ProductA, Rev 100, Year 2021
	// Row 1: Germany, ProductB, Rev 200, Year 2021
	// Row 2: France,  ProductA, Rev 50,  Year 2022

	store := &ColumnStore{
		// Data
		Revenues:   []float64{100.0, 200.0, 50.0},
		Dates:      []int32{202101, 202102, 202205}, // Jan 21, Feb 21, May 22
		Quantities: []int32{1, 2, 1},
		Stocks:     []int32{10, 20, 10},

		// IDs
		CountryIDs: []int32{0, 0, 1}, // 0=Germany, 1=France
		RegionIDs:  []int32{0, 0, 1},
		ProductIDs: []int32{0, 1, 0}, // 0=ProdA, 1=ProdB

		// Dictionaries
		CountryDict: []string{"Germany", "France"},
		RegionDict:  []string{"EU-West", "EU-South"},
		ProductDict: []string{"ProductA", "ProductB"},
	}

	// 2. Run Aggregation
	data := store.Aggregate()

	// 3. Assertions

	// A. Country Stats
	// Should satisfy: Germany Rev=300 (100+200), France Rev=50
	// Germany should be first (Highest revenue)
	if len(data.CountryStats) != 2 {
		t.Fatalf("Expected 2 country stats, got %d", len(data.CountryStats))
	}

	// Check Sort Order & Values
	topC := data.CountryStats[0]
	if topC.Country != "Germany" {
		t.Errorf("Expected top country Germany, got %s", topC.Country)
	}
	if topC.Revenue != 300.0 {
		t.Errorf("Expected Germany Revenue 300.0, got %f", topC.Revenue)
	}
	if topC.Transactions != 2 {
		t.Errorf("Expected Germany Tx 2, got %d", topC.Transactions)
	}

	// B. Monthly Sales (Split by Year)
	// 2021: Jan(100) + Feb(200)
	// 2022: May(50)

	sales2021, exists := data.MonthlySales["2021"]
	if !exists {
		t.Fatal("Missing 2021 data")
	}
	if len(sales2021) != 2 {
		t.Errorf("Expected 2 months in 2021, got %d", len(sales2021))
	}

	if sales2021[0].Month != "January" || sales2021[0].Volume != 100.0 {
		t.Error("2021 January data incorrect")
	}

	sales2022, exists := data.MonthlySales["2022"]
	if !exists {
		t.Fatal("Missing 2022 data")
	}
	if sales2022[0].Month != "May" || sales2022[0].Volume != 50.0 {
		t.Error("2022 May data incorrect")
	}

	// C. Top Products
	// ProdA: Sold 1 (Germany) + 1 (France) = 2. Revenue isn't tracked here, just Qty?
	// Note: TopProducts logic uses 'Value' as Qty sold in our previous code?
	// Let's check the aggregator logic: "addFloat64(&prodSold[pid], float64(qty))" -> Yes, it sums Quantity.

	// ProdA (ID 0) Qty = 1 + 1 = 2
	// ProdB (ID 1) Qty = 2
	// Tie logic depends on sort, but let's check values.

	foundA := false
	for _, p := range data.TopProducts {
		if p.Name == "ProductA" {
			if p.Value != 2.0 {
				t.Errorf("ProdA Qty expected 2.0, got %f", p.Value)
			}
			foundA = true
		}
	}
	if !foundA {
		t.Error("ProductA missing from top products")
	}
}
