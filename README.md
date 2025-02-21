# BDataMatrix - Structured Tabular Data Management in Go

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/bearaujus/bdatamatrix)](https://goreportcard.com/report/github.com/bearaujus/bdatamatrix)

BDataMatrix is a lightweight Go library for managing structured tabular data efficiently. It provides functions to add, update, delete, sort, and query data, along with various export options such as CSV, TSV, JSON, and YAML.

## Installation

To install BDataMatrix, run:

```sh
go get github.com/bearaujus/bdatamatrix
```

## Import

```go
import "github.com/bearaujus/bdatamatrix"
```

## Features

- Create structured tabular data with defined headers.
- Add, update, delete, and search rows efficiently.
- Export data to CSV, TSV, JSON, YAML, or custom formats.
- Track header indices for optimized querying.
- Support for case-insensitive searching.

## Usage

### 1. Creating a Matrix

Create a new matrix with headers:

```go
matrix, err := bdatamatrix.New("ID", "Name", "Age")
if err != nil {
    log.Fatal(err)
}
```

Create a matrix with predefined data:

```go
rows := [][]string{
    {"1", "Alice", "30"},
    {"2", "Bob", "25"},
}
matrix, err := bdatamatrix.NewWithData(rows, "ID", "Name", "Age")
if err != nil {
    log.Fatal(err)
}
```

### 2. Adding and Querying Rows

```go
_ = matrix.AddRow("3", "Charlie", "35")

query := bdatamatrix.FindRowsQuery{
    Column:          "Name",
    Operator:        bdatamatrix.OperatorEquals,
    CaseInsensitive: true,
    Values:          []string{"Alice"},
}

result, err := matrix.FindRows(query)
if err != nil {
    log.Fatal(err)
}
fmt.Println("Matched rows:", result)
```

### 3. Exporting Data

Export as CSV:

```go
csvOutput := matrix.ToCSV(true)
_ = csvOutput.Write("output.csv", 0644)
```

Export as JSON:

```go
jsonOutput := matrix.ToJSON(true, false)
_ = jsonOutput.Write("output.json", 0644)
```

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/bearaujus/bdatamatrix/blob/master/LICENSE) file for details.
