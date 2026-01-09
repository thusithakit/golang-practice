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

// --- 1. FAST ZERO-ALLOC PARSERS ---

func unsafeToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// fastInt parses "123" -> 123
func fastInt(b []byte) int32 {
	var n int32
	for _, c := range b {
		n = n*10 + int32(c-'0')
	}
	return n
}

// fastFloat parses "123.45" -> 123.45
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

// fastDate parses "2021-07-25" -> 202107 (YYYYMM)
func fastDate(b []byte) int32 {
	if len(b) < 7 {
		return 0
	}
	// Optimistically assumes fixed format YYYY-MM-DD
	y := int32(b[0]-'0')*1000 + int32(b[1]-'0')*100 + int32(b[2]-'0')*10 + int32(b[3]-'0')
	m := int32(b[5]-'0')*10 + int32(b[6]-'0')
	return y*100 + m
}

// --- 2. MAIN LOADER ---

func LoadColumnar(path string) *ColumnStore {
	start := time.Now()
	log.Println("Loading data (Parallel Unrolled)...")

	// A. Read File
	content, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	// --- FIX: SKIP HEADER ROW ---
	// We advance the slice past the first newline character.
	if idx := bytes.IndexByte(content, '\n'); idx != -1 {
		content = content[idx+1:]
	}
	// ----------------------------

	numWorkers := runtime.NumCPU()

	// B. Count Rows (Parallel) for Exact Allocation
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

	// C. Allocate Store ONCE
	store := &ColumnStore{
		Revenues:   make([]float64, totalRows),
		Dates:      make([]int32, totalRows),
		Quantities: make([]int32, totalRows),
		Stocks:     make([]int32, totalRows),
		CountryIDs: make([]int32, totalRows),
		RegionIDs:  make([]int32, totalRows),
		ProductIDs: make([]int32, totalRows),
	}

	offsets := make([]int, numWorkers)
	curr := 0
	for i, c := range rowCounts {
		offsets[i] = curr
		curr += c
	}

	// D. Parallel Parsing (Unrolled)
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

	// Pre-define separator for bytes.Cut
	sep := []byte{','}

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

			// HOT LOOP: Unrolled Parsing using bytes.Cut
			for pos < len(chunk) {
				// 1. Find line end
				nextPos := -1
				if i := bytes.IndexByte(chunk[pos:], '\n'); i != -1 {
					nextPos = pos + i
				} else {
					nextPos = len(chunk)
				}

				line := chunk[pos:nextPos]
				pos = nextPos + 1

				if len(line) == 0 {
					continue
				}

				// 2. Unrolled Field Skipping
				// We "hop" over fields. No Loop. No Switch.

				var field []byte
				var rest = line
				var found bool

				// 0: Transaction ID (SKIP)
				if _, rest, found = bytes.Cut(rest, sep); !found {
					continue
				}

				// 1: Date (KEEP)
				if field, rest, found = bytes.Cut(rest, sep); found {
					store.Dates[writeOffset+row] = fastDate(field)
				}

				// 2: User ID (SKIP)
				if _, rest, found = bytes.Cut(rest, sep); !found {
					continue
				}

				// 3: Country (KEEP)
				if field, rest, found = bytes.Cut(rest, sep); found {
					s := unsafeToString(field)
					if id, ok := ld.cMap[s]; ok {
						ld.idsC[row] = id
					} else {
						id = int32(len(ld.cList))
						str := string(field) // Allocate string for dict
						ld.cList = append(ld.cList, str)
						ld.cMap[str] = id
						ld.idsC[row] = id
					}
				}

				// 4: Region (KEEP)
				if field, rest, found = bytes.Cut(rest, sep); found {
					s := unsafeToString(field)
					if id, ok := ld.rMap[s]; ok {
						ld.idsR[row] = id
					} else {
						id = int32(len(ld.rList))
						str := string(field)
						ld.rList = append(ld.rList, str)
						ld.rMap[str] = id
						ld.idsR[row] = id
					}
				}

				// 5: Product ID (SKIP - we use Name)
				if _, rest, found = bytes.Cut(rest, sep); !found {
					continue
				}

				// 6: Product Name (KEEP)
				if field, rest, found = bytes.Cut(rest, sep); found {
					s := unsafeToString(field)
					if id, ok := ld.pMap[s]; ok {
						ld.idsP[row] = id
					} else {
						id = int32(len(ld.pList))
						str := string(field)
						ld.pList = append(ld.pList, str)
						ld.pMap[str] = id
						ld.idsP[row] = id
					}
				}

				// 7: Category (SKIP)
				if _, rest, found = bytes.Cut(rest, sep); !found {
					continue
				}

				// 8: Price (SKIP)
				if _, rest, found = bytes.Cut(rest, sep); !found {
					continue
				}

				// 9: Quantity (KEEP)
				if field, rest, found = bytes.Cut(rest, sep); found {
					store.Quantities[writeOffset+row] = fastInt(field)
				}

				// 10: Total Revenue (KEEP)
				if field, rest, found = bytes.Cut(rest, sep); found {
					store.Revenues[writeOffset+row] = fastFloat(field)
				}

				// 11: Stock (KEEP - Last value before end or comma)
				// Note: if there is a 12th column (added_date), we must Cut again.
				// Based on sample, there is a 12th column. So we Cut.
				if field, rest, found = bytes.Cut(rest, sep); found {
					store.Stocks[writeOffset+row] = fastInt(field)
				} else {
					// Fallback if 12th col is missing
					store.Stocks[writeOffset+row] = fastInt(rest)
				}

				// 12: Added Date (Implicitly Skipped)

				row++
			}
		}(i, i*chunkSize, (i+1)*chunkSize, offsets[i])
	}
	parseWg.Wait()

	// E. Merge Dictionaries (Parallel)
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
