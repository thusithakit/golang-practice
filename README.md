# 🚀 High-Performance Go Analytics Engine

![Go Version](https://img.shields.io/badge/go-1.22+-00ADD8?style=flat&logo=go)
![Performance](https://img.shields.io/badge/performance-~1.8s-brightgreen)
![Architecture](https://img.shields.io/badge/architecture-columnar_store-orange)

A high-throughput ETL engine built from scratch in Go. It ingests a **500MB+ dataset (5 million rows)**, performs complex in-memory aggregations, and serves real-time analytics via a REST API—all in **under 2 seconds**.

## ⚡ Key Performance Metrics

| Metric | Standard Approach | This Engine | Improvement |
| :--- | :--- | :--- | :--- |
| **Load Time** | ~12.0s (encoding/csv) | **~1.5s** | **8x Faster** |
| **Aggregation** | ~4.0s (Map Lookups) | **~0.3s** | **13x Faster** |
| **Memory** | ~2.5GB (Structs) | **~350MB** | **Low GC Pressure** |

---

## 🏗 Architecture

This project moves away from row-based processing and implements a custom **Columnar In-Memory Store** (similar to Apache Arrow or ClickHouse logic) to maximize CPU cache efficiency.



### 1. The Loader (`internal/engine/loader.go`)
* **Parallel Gather Pattern:** The file is split into chunks processed by concurrent workers.
* **Custom Parsers:** Replaces the standard `strconv` library with custom integer-based parsing for dates and currency (cents), achieving a 4x speedup.
* **Dictionary Encoding:** High-cardinality strings (Country, Region, Product) are compressed into `int32` IDs, reducing memory usage and enabling fast array-based lookups.

### 2. The Aggregator (`internal/engine/aggregator.go`)
* **Flat Array Aggregation:** Instead of slow Go Maps (`map[string]float64`), we use flat arrays indexed by the Integer IDs generated during loading.
* **Atomic Operations:** Uses `sync/atomic` (Compare-And-Swap) to allow safe, lock-free updates to the global aggregation matrix from multiple threads.
* **Zero Allocation:** All aggregation arrays are pre-allocated based on the dictionary size, eliminating Garbage Collection pauses during the compute phase.

### 3. The API (`internal/api`)
* A lightweight `Echo` server wraps the pre-calculated results.
* Since the heavy lifting is done at startup, API endpoints respond in **sub-millisecond** time.

---

## 📂 Project Structure

```text
/abt-dashboard
├── cmd
│   └── server
│       └── main.go           # Entry point (Orchestrator)
├── internal
│   ├── api
│   │   └── handlers.go       # HTTP Handlers & Routes
│   ├── engine
│   │   ├── loader.go         # Parallel CSV Parsing & Dictionary Encoding
│   │   ├── aggregator.go     # Atomic Map-Reduce Logic
│   │   └── store.go          # Columnar Data Structure
│   └── models
│       └── models.go         # JSON Response Structures
├── GO_Test.csv               # Dataset (Not included)
├── go.mod
└── README.md