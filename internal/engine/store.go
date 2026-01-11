package engine

type ColumnStore struct {
	Revenues   []float64
	Dates      []int32
	Quantities []int32
	Stocks     []int32

	CountryIDs []int32
	RegionIDs  []int32
	ProductIDs []int32

	CountryDict []string
	RegionDict  []string
	ProductDict []string
}
