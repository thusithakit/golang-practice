package engine

import (
	"os"
	"testing"
)

func TestLoadColumnar(t *testing.T) {
	csvContent := []byte(`transaction_id,transaction_date,user_id,country,region,product_id,product_name,category,price,quantity,total_price,stock_quantity,added_date
T1,2021-01-15,U1,Germany,Bavaria,P1,Widget_A,Toys,10.50,2,21.00,100,2021-01-01
T2,2021-01-16,U2,France,Normandy,P2,Widget_B,Toys,20.00,1,20.00,50,2021-01-02
T3,2022-05-20,U3,Germany,Hesse,P1,Widget_A,Toys,10.50,1,10.50,99,2022-05-05
`)

	tmpFile, err := os.CreateTemp("", "test_data_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(csvContent); err != nil {
		t.Fatal(err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatal(err)
	}

	// 2. Run Loader
	store := LoadColumnar(tmpFile.Name())

	// 3. Assertions

	// Expect 3 rows now
	if len(store.Revenues) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(store.Revenues))
	}

	// Row 0 Check
	if store.Revenues[0] != 21.0 {
		t.Errorf("Row 0 Revenue: Expected 21.0, got %f", store.Revenues[0])
	}
	if store.Dates[0] != 202101 {
		t.Errorf("Row 0 Date: Expected 202101, got %d", store.Dates[0])
	}

	// Dictionary Checks
	if len(store.CountryDict) != 2 {
		t.Errorf("Expected 2 unique countries, got %d", len(store.CountryDict))
	}
}

func TestFastHelpers(t *testing.T) {
	f := fastFloat([]byte("123.45"))
	if f != 123.45 {
		t.Errorf("fastFloat failed: %v", f)
	}

	i := fastInt([]byte("99"))
	if i != 99 {
		t.Errorf("fastInt failed: %v", i)
	}

	d := fastDate([]byte("2023-12-01"))
	if d != 202312 {
		t.Errorf("fastDate failed: %v", d)
	}
}
