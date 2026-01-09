package engine

// ColumnStore holds data in Struct-of-Arrays format for speed
type ColumnStore struct {
	// Data Columns (Flat Arrays)
	Revenues   []float64
	Dates      []int32
	Quantities []int32
	Stocks     []int32

	// Dictionary Encoded IDs (0..N)
	CountryIDs []int32
	RegionIDs  []int32
	ProductIDs []int32

	// Dictionaries (ID -> String)
	CountryDict []string
	RegionDict  []string
	ProductDict []string
}
