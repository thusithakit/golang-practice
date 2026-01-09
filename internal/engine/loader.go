package engine

import (
	"bytes"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

// --- Fast Parsers ---
func unsafeToString(b []byte) string { return *(*string)(unsafe.Pointer(&b)) }
func fastInt(b []byte) int32 {
	var n int32
	for _, c := range b {
		n = n*10 + int32(c-'0')
	}
	return n
}
func fastFloat(b []byte) float64 {
	var num float64
	var i int
	for i < len(b) && b[i] != '.' {
		num = num*10 + float64(b[i]-'0')
		i++
	}
	if i < len(b) {
		i++
		div := 10.0
		for i < len(b) {
			num += float64(b[i]-'0') / div
			div *= 10
			i++
		}
	}
	return num
}
func fastDate(b []byte) int32 {
	if len(b) < 7 {
		return 0
	}
	// Expects YYYY-MM-DD -> YYYYMM
	y := int32(b[0]-'0')*1000 + int32(b[1]-'0')*100 + int32(b[2]-'0')*10 + int32(b[3]-'0')
	m := int32(b[5]-'0')*10 + int32(b[6]-'0')
	return y*100 + m
}

// --- Parallel Loader ---
func LoadColumnar(path string) *ColumnStore {
	start := time.Now()
	log.Println("Loading data (Parallel Columnar)...")

	// 1. Read File
	content, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	numWorkers := runtime.NumCPU()

	// 2. Count Rows (Parallel) for Exact Allocation
	chunkSize := len(content) / numWorkers
	rowCounts := make([]int, numWorkers)
	var countWg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		countWg.Add(1)
		go func(idx int, start, end int) {
			defer countWg.Done()
			// Align to newlines
			if start > 0 {
				if i := bytes.IndexByte(content[start:], '\n'); i != -1 {
					start += i + 1
				}
			}
			if end < len(content) {
				if i := bytes.IndexByte(content[end:], '\n'); i != -1 {
					end += i + 1
				} else {
					end = len(content)
				}
			}
			if start < end {
				rowCounts[idx] = bytes.Count(content[start:end], []byte{'\n'})
			}
		}(i, i*chunkSize, (i+1)*chunkSize)
	}
	countWg.Wait()

	totalRows := 0
	for _, c := range rowCounts {
		totalRows += c
	}

	// 3. Allocate Store ONCE
	store := &ColumnStore{
		Revenues:   make([]float64, totalRows),
		Dates:      make([]int32, totalRows),
		Quantities: make([]int32, totalRows),
		Stocks:     make([]int32, totalRows),
		CountryIDs: make([]int32, totalRows),
		RegionIDs:  make([]int32, totalRows),
		ProductIDs: make([]int32, totalRows),
	}

	// Calculate write offsets
	offsets := make([]int, numWorkers)
	curr := 0
	for i, c := range rowCounts {
		offsets[i] = curr
		curr += c
	}

	// 4. Parse & Fill (Parallel)
	type localDicts struct {
		cMap  map[string]int32
		cList []string
		rMap  map[string]int32
		rList []string
		pMap  map[string]int32
		pList []string
		idsC  []int32
		idsR  []int32
		idsP  []int32
	}
	workerDicts := make([]*localDicts, numWorkers)

	var parseWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		parseWg.Add(1)
		go func(idx int, start, end int, writeOffset int) {
			defer parseWg.Done()

			// Init local dicts
			ld := &localDicts{
				cMap: make(map[string]int32), rMap: make(map[string]int32), pMap: make(map[string]int32),
				idsC: make([]int32, rowCounts[idx]), idsR: make([]int32, rowCounts[idx]), idsP: make([]int32, rowCounts[idx]),
			}
			workerDicts[idx] = ld

			// Align chunk again
			if start > 0 {
				if i := bytes.IndexByte(content[start:], '\n'); i != -1 {
					start += i + 1
				}
			}
			if end < len(content) {
				if i := bytes.IndexByte(content[end:], '\n'); i != -1 {
					end += i + 1
				} else {
					end = len(content)
				}
			}

			chunk := content[start:end]
			pos := 0
			row := 0
			var val []byte
			var colIdx, nextPos int

			for pos < len(chunk) {
				nextPos = -1
				for k := pos; k < len(chunk); k++ {
					if chunk[k] == '\n' {
						nextPos = k
						break
					}
				}
				if nextPos == -1 {
					nextPos = len(chunk)
				}

				line := chunk[pos:nextPos]
				pos = nextPos + 1
				if len(line) == 0 {
					continue
				}

				colIdx = 0
				lastScan := 0
				for k := 0; k <= len(line); k++ {
					if k == len(line) || line[k] == ',' {
						val = line[lastScan:k]
						switch colIdx {
						case 1:
							store.Dates[writeOffset+row] = fastDate(val)
						case 3:
							s := unsafeToString(val) // Safe because 'content' lives forever
							if id, ok := ld.cMap[s]; ok {
								ld.idsC[row] = id
							} else {
								id = int32(len(ld.cList))
								// Allocate string here for the dictionary
								str := string(val)
								ld.cList = append(ld.cList, str)
								ld.cMap[str] = id
								ld.idsC[row] = id
							}
						case 4:
							s := unsafeToString(val)
							if id, ok := ld.rMap[s]; ok {
								ld.idsR[row] = id
							} else {
								id = int32(len(ld.rList))
								str := string(val)
								ld.rList = append(ld.rList, str)
								ld.rMap[str] = id
								ld.idsR[row] = id
							}
						case 6:
							s := unsafeToString(val)
							if id, ok := ld.pMap[s]; ok {
								ld.idsP[row] = id
							} else {
								id = int32(len(ld.pList))
								str := string(val)
								ld.pList = append(ld.pList, str)
								ld.pMap[str] = id
								ld.idsP[row] = id
							}
						case 9:
							store.Quantities[writeOffset+row] = fastInt(val)
						case 10:
							store.Revenues[writeOffset+row] = fastFloat(val)
						case 11:
							store.Stocks[writeOffset+row] = fastInt(val)
						}
						lastScan = k + 1
						colIdx++
					}
				}
				row++
			}
		}(i, i*chunkSize, (i+1)*chunkSize, offsets[i])
	}
	parseWg.Wait()

	// 5. Merge Dictionaries
	var dictWg sync.WaitGroup
	dictWg.Add(3)

	mergeDict := func(getList func(*localDicts) []string, getIDs func(*localDicts) []int32, globalDict *[]string, globalIDs []int32) {
		defer dictWg.Done()
		gMap := make(map[string]int32)
		*globalDict = make([]string, 0, 1000)
		remaps := make([][]int32, numWorkers)

		for w := 0; w < numWorkers; w++ {
			localList := getList(workerDicts[w])
			remaps[w] = make([]int32, len(localList))
			for lid, s := range localList {
				if gid, exists := gMap[s]; exists {
					remaps[w][lid] = gid
				} else {
					gid = int32(len(*globalDict))
					*globalDict = append(*globalDict, s)
					gMap[s] = gid
					remaps[w][lid] = gid
				}
			}
		}
		for w := 0; w < numWorkers; w++ {
			localIDs := getIDs(workerDicts[w])
			dest := globalIDs[offsets[w] : offsets[w]+len(localIDs)]
			remap := remaps[w]
			for k, id := range localIDs {
				dest[k] = remap[id]
			}
		}
	}

	go mergeDict(func(d *localDicts) []string { return d.cList }, func(d *localDicts) []int32 { return d.idsC }, &store.CountryDict, store.CountryIDs)
	go mergeDict(func(d *localDicts) []string { return d.rList }, func(d *localDicts) []int32 { return d.idsR }, &store.RegionDict, store.RegionIDs)
	go mergeDict(func(d *localDicts) []string { return d.pList }, func(d *localDicts) []int32 { return d.idsP }, &store.ProductDict, store.ProductIDs)

	dictWg.Wait()
	log.Printf("Load Complete. Rows: %d. Time: %v", totalRows, time.Since(start))
	return store
}
