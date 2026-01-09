package engine

import (
	"backend/internal/models"
	"fmt"
	"runtime"
	"sort"
	"sync"
)

type aggStats struct {
	Rev   float64
	Trans int
}

func (cs *ColumnStore) Aggregate() *models.DashboardData {
	// 1. Dimensions
	// We need these to calculate array offsets
	numProds := len(cs.ProductDict)
	numRegs := len(cs.RegionDict)
	numCountries := len(cs.CountryDict)
	numDates := 300000 // Covers dates like 202512. Max index 300,000 is safe/small (~2MB)

	// 2. Setup Workers
	numWorkers := runtime.NumCPU()
	chunkSize := len(cs.Dates) / numWorkers

	// Safety Check: If matrix is too huge (e.g. 1GB+ per worker), we might OOM.
	// 200 Countries * 50,000 Products = 10M entries = 160MB per worker. Acceptable.
	matrixSize := numCountries * numProds

	type partialAgg struct {
		prodSold  []float64
		prodStock []int
		regRev    []float64
		regSold   []int
		monthRev  []float64 // Array instead of Map for speed

		// THE MATRIX: Flattened [Country][Product] -> [Country * NumProds + Product]
		// This replaces ctryMap
		matrix []aggStats
	}

	results := make(chan *partialAgg, numWorkers)
	var wg sync.WaitGroup

	// 3. Parallel Loop (Branchless)
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = len(cs.Dates)
		}

		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()

			// Allocation: We alloc once per worker.
			// This is faster than 5 million map inserts.
			p := &partialAgg{
				prodSold:  make([]float64, numProds),
				prodStock: make([]int, numProds),
				regRev:    make([]float64, numRegs),
				regSold:   make([]int, numRegs),
				monthRev:  make([]float64, numDates), // Assuming YYYYMM fits in int32 bounds logic
				matrix:    make([]aggStats, matrixSize),
			}

			// Capture slice headers to avoid bounds checks in loop
			idsP := cs.ProductIDs
			idsR := cs.RegionIDs
			idsC := cs.CountryIDs
			revs := cs.Revenues
			qtys := cs.Quantities
			stks := cs.Stocks
			dates := cs.Dates

			// Date offset adjustment (e.g., 202001 -> index)
			// Simple hack: index = date % 10000 (MM) + (date/10000 * 12)?
			// For raw speed, let's just use raw date integer as index minus a base year.
			// Assuming Date is int32 like 202107.
			// We'll trust the array size covers it or mask it.
			// Let's use a safe base to keep array small: 200000.
			baseDate := int32(200000)

			for j := s; j < e; j++ {
				// Load Data
				pid := idsP[j]
				rid := idsR[j]
				cid := idsC[j]
				rev := revs[j]
				qty := qtys[j]

				// A. Products (Array Indexing)
				p.prodSold[pid] += float64(qty)
				p.prodStock[pid] = int(stks[j]) // Overwrite is faster than check

				// B. Regions (Array Indexing)
				p.regRev[rid] += rev
				p.regSold[rid] += int(qty)

				// C. Month (Array Indexing)
				dIdx := dates[j] - baseDate
				if dIdx >= 0 && int(dIdx) < len(p.monthRev) {
					p.monthRev[dIdx] += rev
				}

				// D. MATRIX UPDATE (The Magic)
				// Replaces: map[key] += val
				// No Hashing. No Collisions. Just Math.
				idx := int(cid)*numProds + int(pid)
				p.matrix[idx].Rev += rev
				p.matrix[idx].Trans++
			}
			results <- p
		}(start, end)
	}

	go func() { wg.Wait(); close(results) }()

	// 4. Merge Phase (Reducer)
	// We aggregate the partial arrays into final arrays
	finalProdSold := make([]float64, numProds)
	finalProdStock := make([]int, numProds)
	finalRegRev := make([]float64, numRegs)
	finalRegSold := make([]int, numRegs)
	finalMonthMap := make(map[string]float64) // Convert back to string map here
	finalMatrix := make([]aggStats, matrixSize)

	for p := range results {
		// Vectorized Summation
		for i := 0; i < numProds; i++ {
			finalProdSold[i] += p.prodSold[i]
			if finalProdStock[i] == 0 {
				finalProdStock[i] = p.prodStock[i]
			}
		}
		for i := 0; i < numRegs; i++ {
			finalRegRev[i] += p.regRev[i]
			finalRegSold[i] += p.regSold[i]
		}

		// Merge Matrix
		for i := 0; i < matrixSize; i++ {
			if p.matrix[i].Trans > 0 { // Optimization: Skip empty zeroes
				finalMatrix[i].Rev += p.matrix[i].Rev
				finalMatrix[i].Trans += p.matrix[i].Trans
			}
		}

		// Merge Dates (Convert array back to map structure for output)
		for i, v := range p.monthRev {
			if v > 0 {
				realDate := int32(i) + 200000
				mStr := fmt.Sprintf("%d-%02d", realDate/100, realDate%100)
				finalMonthMap[mStr] += v
			}
		}
	}

	// 5. Build Result (DashboardData)
	data := &models.DashboardData{
		TopProducts:  make([]models.TopItem, 0),
		TopRegions:   make([]models.TopItem, 0),
		MonthlySales: make([]models.MonthlyItem, 0),
		CountryTable: make([]models.CountryRow, 0),
	}

	// Top Products
	for i, sold := range finalProdSold {
		if sold > 0 {
			data.TopProducts = append(data.TopProducts, models.TopItem{
				Name: cs.ProductDict[i], Value: sold, Extra: finalProdStock[i],
			})
		}
	}
	// Sort Products
	sort.Slice(data.TopProducts, func(i, j int) bool { return data.TopProducts[i].Value > data.TopProducts[j].Value })
	if len(data.TopProducts) > 20 {
		data.TopProducts = data.TopProducts[:20]
	}

	// Top Regions
	for i, rev := range finalRegRev {
		if rev > 0 {
			data.TopRegions = append(data.TopRegions, models.TopItem{
				Name: cs.RegionDict[i], Value: rev, Extra: finalRegSold[i],
			})
		}
	}
	// Sort Regions
	sort.Slice(data.TopRegions, func(i, j int) bool { return data.TopRegions[i].Value > data.TopRegions[j].Value })
	if len(data.TopRegions) > 30 {
		data.TopRegions = data.TopRegions[:30]
	}

	// Monthly Sales
	for m, vol := range finalMonthMap {
		data.MonthlySales = append(data.MonthlySales, models.MonthlyItem{Month: m, Volume: vol})
	}
	// Sort Monthly
	sort.Slice(data.MonthlySales, func(i, j int) bool { return data.MonthlySales[i].Month < data.MonthlySales[j].Month })

	// Country Table (Unpack Matrix)
	for i, stats := range finalMatrix {
		if stats.Trans > 0 {
			// Reverse Math: index -> cid, pid
			cid := i / numProds
			pid := i % numProds

			data.CountryTable = append(data.CountryTable, models.CountryRow{
				Country:      cs.CountryDict[cid],
				Product:      cs.ProductDict[pid],
				Revenue:      stats.Rev,
				Transactions: stats.Trans,
			})
		}
	}
	// Sort Table
	sort.Slice(data.CountryTable, func(i, j int) bool { return data.CountryTable[i].Revenue > data.CountryTable[j].Revenue })

	return data
}
