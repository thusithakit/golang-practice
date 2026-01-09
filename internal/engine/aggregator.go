package engine

import (
	"backend/internal/models"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Atomic Helpers
func addFloat64(val *float64, delta float64) {
	for {
		old := math.Float64frombits(atomic.LoadUint64((*uint64)(unsafe.Pointer(val))))
		newVal := old + delta
		if atomic.CompareAndSwapUint64((*uint64)(unsafe.Pointer(val)), math.Float64bits(old), math.Float64bits(newVal)) {
			break
		}
	}
}

func (cs *ColumnStore) Aggregate() *models.DashboardData {
	// Dimensions
	numProds := len(cs.ProductDict)
	numRegs := len(cs.RegionDict)
	numCountries := len(cs.CountryDict)

	// Pre-allocate Global Arrays (Zero Allocation during loop)
	prodSold := make([]float64, numProds)
	prodStock := make([]int64, numProds)
	regRev := make([]float64, numRegs)
	regSold := make([]int64, numRegs)
	// Matrix: [Country * NumProds + Product]
	matrixRev := make([]float64, numCountries*numProds)
	matrixTx := make([]int64, numCountries*numProds)

	// Workers
	numWorkers := runtime.NumCPU()
	chunkSize := len(cs.Dates) / numWorkers

	// Local Month Maps (to avoid atomic contention on small date sets)
	monthPartial := make([]map[int32]float64, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		monthPartial[i] = make(map[int32]float64)
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = len(cs.Dates)
		}

		wg.Add(1)
		go func(idx, s, e int) {
			defer wg.Done()
			localMonths := monthPartial[idx]
			// Hoist slices
			idsP := cs.ProductIDs
			idsR := cs.RegionIDs
			idsC := cs.CountryIDs
			revs := cs.Revenues
			qtys := cs.Quantities
			stks := cs.Stocks
			dates := cs.Dates

			for j := s; j < e; j++ {
				pid := idsP[j]
				rid := idsR[j]
				rev := revs[j]
				qty := qtys[j]

				// Atomic Updates
				addFloat64(&prodSold[pid], float64(qty))
				// Simple atomic store for stock (Last-Write-Wins is fine for snapshot)
				atomic.StoreInt64(&prodStock[pid], int64(stks[j]))

				addFloat64(&regRev[rid], rev)
				atomic.AddInt64(&regSold[rid], int64(qty))

				// Matrix
				midx := int(idsC[j])*numProds + int(pid)
				addFloat64(&matrixRev[midx], rev)
				atomic.AddInt64(&matrixTx[midx], 1)

				// Local Month
				localMonths[dates[j]] += rev
			}
		}(i, start, end)
	}
	wg.Wait()

	// --- Finalize & Format ---
	data := &models.DashboardData{
		TopProducts:  make([]models.TopItem, 0),
		TopRegions:   make([]models.TopItem, 0),
		MonthlySales: make([]models.MonthlyItem, 0),
		CountryTable: make([]models.CountryRow, 0),
	}

	// 1. Products
	for i, v := range prodSold {
		if v > 0 {
			data.TopProducts = append(data.TopProducts, models.TopItem{
				Name: cs.ProductDict[i], Value: v, Extra: int(prodStock[i]),
			})
		}
	}
	sort.Slice(data.TopProducts, func(i, j int) bool { return data.TopProducts[i].Value > data.TopProducts[j].Value })
	if len(data.TopProducts) > 20 {
		data.TopProducts = data.TopProducts[:20]
	}

	// 2. Regions
	for i, v := range regRev {
		if v > 0 {
			data.TopRegions = append(data.TopRegions, models.TopItem{
				Name: cs.RegionDict[i], Value: v, Extra: int(regSold[i]),
			})
		}
	}
	sort.Slice(data.TopRegions, func(i, j int) bool { return data.TopRegions[i].Value > data.TopRegions[j].Value })
	if len(data.TopRegions) > 30 {
		data.TopRegions = data.TopRegions[:30]
	}

	// 3. Months (Merge Locals)
	mergedMonths := make(map[int32]float64)
	for _, m := range monthPartial {
		for k, v := range m {
			mergedMonths[k] += v
		}
	}
	for k, v := range mergedMonths {
		mStr := fmt.Sprintf("%d-%02d", k/100, k%100)
		data.MonthlySales = append(data.MonthlySales, models.MonthlyItem{Month: mStr, Volume: v})
	}
	sort.Slice(data.MonthlySales, func(i, j int) bool { return data.MonthlySales[i].Month < data.MonthlySales[j].Month })

	// 4. Country Table (Unpack Matrix)
	for i, tx := range matrixTx {
		if tx > 0 {
			rev := matrixRev[i]
			cid := i / numProds
			pid := i % numProds
			data.CountryTable = append(data.CountryTable, models.CountryRow{
				Country: cs.CountryDict[cid], Product: cs.ProductDict[pid], Revenue: rev, Transactions: int(tx),
			})
		}
	}
	sort.Slice(data.CountryTable, func(i, j int) bool { return data.CountryTable[i].Revenue > data.CountryTable[j].Revenue })

	return data
}
