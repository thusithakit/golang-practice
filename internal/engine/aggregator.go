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

var monthNames = [...]string{
	"", "January", "February", "March", "April", "May", "June",
	"July", "August", "September", "October", "November", "December",
}

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

	prodSold := make([]float64, numProds)
	prodStock := make([]int64, numProds)
	regRev := make([]float64, numRegs)
	regSold := make([]int64, numRegs)
	ctryRev := make([]float64, numCountries)
	ctryTx := make([]int64, numCountries)

	// Workers
	numWorkers := runtime.NumCPU()
	chunkSize := len(cs.Dates) / numWorkers

	// NEW: Local map key is Date Int (YYYYMM)
	// We aggregate by specific YYYYMM first, then split later
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
				cid := idsC[j]
				rev := revs[j]
				qty := qtys[j]

				// 1. Products
				addFloat64(&prodSold[pid], float64(qty))
				atomic.StoreInt64(&prodStock[pid], int64(stks[j]))

				// 2. Regions
				addFloat64(&regRev[rid], rev)
				atomic.AddInt64(&regSold[rid], int64(qty))

				// 3. Countries
				addFloat64(&ctryRev[cid], rev)
				atomic.AddInt64(&ctryTx[cid], 1)

				// 4. Monthly (Store full YYYYMM)
				localMonths[dates[j]] += rev
			}
		}(i, start, end)
	}
	wg.Wait()

	// --- Finalize ---
	data := &models.DashboardData{
		CountryStats: make([]models.CountryStat, 0),
		TopProducts:  make([]models.TopItem, 0),
		TopRegions:   make([]models.TopItem, 0),
		MonthlySales: make(map[string][]models.MonthlyItem), // Map Init
	}

	// 1. Country Stats
	for i, revenue := range ctryRev {
		if revenue > 0 {
			data.CountryStats = append(data.CountryStats, models.CountryStat{
				Country:      cs.CountryDict[i],
				Revenue:      revenue,
				Transactions: int(ctryTx[i]),
			})
		}
	}
	sort.Slice(data.CountryStats, func(i, j int) bool { return data.CountryStats[i].Revenue > data.CountryStats[j].Revenue })

	// 2. Products
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

	// 3. Regions
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

	// 4. Monthly Sales (Split by Year)
	// Merge partials first
	merged := make(map[int32]float64)
	for _, m := range monthPartial {
		for k, v := range m {
			merged[k] += v
		}
	}

	// Group by Year
	// Temporary map: Year -> []Items
	yearMap := make(map[string][]models.MonthlyItem)

	// Sort keys first to ensure chronological insertion
	sortedKeys := make([]int, 0, len(merged))
	for k := range merged {
		sortedKeys = append(sortedKeys, int(k))
	}
	sort.Ints(sortedKeys)

	for _, dateInt := range sortedKeys {
		y := dateInt / 100
		m := dateInt % 100

		yearStr := fmt.Sprintf("%d", y)
		if m >= 1 && m <= 12 {
			yearMap[yearStr] = append(yearMap[yearStr], models.MonthlyItem{
				Month:  monthNames[m],
				Volume: merged[int32(dateInt)],
			})
		}
	}
	data.MonthlySales = yearMap

	return data
}
