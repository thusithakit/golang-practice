package engine

import "sync"

// ColumnStore holds data in Structure-of-Arrays format for cache efficiency
type ColumnStore struct {
	// Dictionary Encoded IDs
	CountryIDs []int32
	ProductIDs []int32
	RegionIDs  []int32

	// Data Columns
	Dates      []int32 // YYYYMM
	Revenues   []float64
	Quantities []int32
	Stocks     []int32

	// Dictionaries (ID -> String)
	CountryDict []string
	ProductDict []string
	RegionDict  []string

	// Fast Lookup Maps (String -> ID) - Used only during loading
	countryMap map[string]int32
	productMap map[string]int32
	regionMap  map[string]int32

	// Mutex for potential concurrent writes if not using the lock-free loader
	dictMu sync.RWMutex
}
