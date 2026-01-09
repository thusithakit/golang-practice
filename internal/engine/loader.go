package engine

import (
	"bytes"
	"log"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// --- 1. SUPER-FAST PARSERS (Same as before) ---
// (Keep fastFloat, fastInt, fastDate, unsafeToString from previous step)
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

func fastInt(b []byte) int32 {
	var n int32
	for _, c := range b {
		n = n*10 + int32(c-'0')
	}
	return n
}

func fastDate(b []byte) int32 {
	if len(b) < 7 {
		return 0
	}
	y := int32(b[0]-'0')*1000 + int32(b[1]-'0')*100 + int32(b[2]-'0')*10 + int32(b[3]-'0')
	m := int32(b[5]-'0')*10 + int32(b[6]-'0')
	return y*100 + m
}

func unsafeToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// --- 2. MMAP LOADER ---

func LoadColumnar(path string) *ColumnStore {
	start := time.Now()
	log.Println("Loading data (MMAP + Parallel)...")

	// A. Open File
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	// Get File Size
	fi, err := f.Stat()
	if err != nil {
		log.Fatal(err)
	}
	size := fi.Size()

	// B. Memory Map (The Magic)
	// PROT_READ: Read Only
	// MAP_SHARED: Shared with other processes (standard for file mapping)
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Fatal("mmap failed:", err)
	}

	// We can close the file handle immediately; the map remains valid.
	f.Close()

	// Defer Unmap to clean up memory when we return (optional, depends on lifecycle)
	// WARNING: If we unmap, any "unsafeString" references will SEGFAULT.
	// Since our Dictionary copies strings (string(val)), we are safe to unmap
	// AFTER we are done parsing.
	defer syscall.Munmap(data)

	// --- C. PARALLEL PARSING (Same Logic, Higher Speed) ---

	numWorkers := runtime.NumCPU()
	chunkSize := len(data) / numWorkers
	rowCounts := make([]int, numWorkers)
	var countWg sync.WaitGroup

	// 1. Count Rows (Parallel)
	for i := 0; i < numWorkers; i++ {
		countWg.Add(1)
		go func(idx int, start, end int) {
			defer countWg.Done()
			if start > 0 {
				if i := bytes.IndexByte(data[start:], '\n'); i != -1 {
					start += i + 1
				}
			}
			if end < len(data) {
				if i := bytes.IndexByte(data[end:], '\n'); i != -1 {
					end += i + 1
				} else {
					end = len(data)
				}
			}
			if start < end {
				rowCounts[idx] = bytes.Count(data[start:end], []byte{'\n'})
			}
		}(i, i*chunkSize, (i+1)*chunkSize)
	}
	countWg.Wait()

	totalRows := 0
	for _, c := range rowCounts {
		totalRows += c
	}

	// 2. Alloc Store
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

	// 3. Local Dicts
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

	// 4. Parse (Parallel)
	var parseWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		parseWg.Add(1)
		go func(idx int, start, end int, writeOffset int) {
			defer parseWg.Done()

			ld := &localDicts{
				cMap: make(map[string]int32), rMap: make(map[string]int32), pMap: make(map[string]int32),
				idsC: make([]int32, rowCounts[idx]), idsR: make([]int32, rowCounts[idx]), idsP: make([]int32, rowCounts[idx]),
			}
			workerDicts[idx] = ld

			if start > 0 {
				if i := bytes.IndexByte(data[start:], '\n'); i != -1 {
					start += i + 1
				}
			}
			if end < len(data) {
				if i := bytes.IndexByte(data[end:], '\n'); i != -1 {
					end += i + 1
				} else {
					end = len(data)
				}
			}

			chunk := data[start:end]
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
							s := unsafeToString(val)
							if id, ok := ld.cMap[s]; ok {
								ld.idsC[row] = id
							} else {
								id = int32(len(ld.cList))
								// IMPORTANT: We MUST copy the string here (string(val))
								// because 'val' points to mmap memory which will be Unmapped later.
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

	// 5. Merge Dicts (Parallel)
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

	log.Printf("MMAP Load Complete. Rows: %d. Time: %v", totalRows, time.Since(start))
	return store
}
