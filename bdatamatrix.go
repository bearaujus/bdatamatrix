package bdatamatrix

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// BDataMatrix defines the behavior for a structured tabular data matrix.
type BDataMatrix interface {
	// AddRow appends a single row to the matrix.
	//
	// Parameters:
	//   - values: The values for the row to be added.
	//
	// Returns:
	//   - An error if writing fails.
	AddRow(values ...string) error

	// AddRows appends multiple rows to the matrix.
	//
	// Parameters:
	//   - values: The values for the multiple rows to be added.
	//
	// Returns:
	//   - An error if writing fails.
	AddRows(rows ...[]string) error

	// GetRow retrieves a row by index.
	//
	// Parameters:
	//   - index: The index of the row to retrieve.
	//
	// Returns:
	// 	 - The value of row at the specified index.
	//   - An error if writing fails.
	GetRow(index int) ([]string, error)

	// GetRows retrieves multiple rows by indexes.
	//
	// Parameters:
	//   - indexes: More than one index of the rows to retrieve.
	//
	// Returns:
	// 	 - The value of rows at the specified indexes.
	//   - An error if writing fails.
	GetRows(indexes ...int) (BDataMatrix, error)

	// GetColumn retrieves a column by name.
	//
	// Parameters:
	//   - key: The name of the column to retrieve.
	//
	// Returns:
	// 	 - The value of column at the specified key.
	//   - An error if writing fails.
	GetColumn(key string) ([]string, error)

	// GetColumns retrieves multiple columns by names.
	//
	// Parameters:
	//   - key: The names of the columns to retrieve.
	//
	// Returns:
	// 	 - The value of columns at the specified keys.
	//   - An error if writing fails.
	GetColumns(keys ...string) (BDataMatrix, error)

	// UpdateRow updates an existing row at the specified index.
	//
	// Parameters:
	//   - index: The index of the row want to be updated.
	//   - values: The value that want to be updated.
	//
	// Returns:
	//   - An error if writing fails.
	UpdateRow(index int, values ...string) error

	// DeleteRow removes a row at the specified index.
	//
	// Parameters:
	//   - index: The index of the row want to be deleted.
	//
	// Returns:
	//   - An error if writing fails.
	DeleteRow(index int) error

	// FindRows searches for rows matching a given query.
	//
	// Parameters:
	//   - query: Have struct FindRowsQuery need to be filled.
	// Returns:
	//   - The value based on parameter query.
	//   - An error if writing fails.
	FindRows(query FindRowsQuery) (BDataMatrix, error)

	// FindRowsWithHistories searches for rows matching a given query with histories.
	//
	// Parameters:
	//   - query: Have struct FindRowsQuery need to be filled.
	// Returns:
	//   - The value based on parameter query and history of match the data.
	//   - An error if writing fails.
	FindRowsWithHistories(query FindRowsQuery) (BDataMatrix, BDataMatrix, error)

	// SortByDesc sorts rows by descending based on one or more column names.
	//
	// Parameters:
	//   - keys : The names of the columns to sorting.
	// Returns:
	//   - An error if writing fails.
	SortByDesc(keys ...string) error

	// SortByDesc sorts rows by ascending based on one or more column names.
	//
	// Parameters:
	//   - keys : The names of the columns to sorting.
	// Returns:
	//   - An error if writing fails.
	SortByAsc(keys ...string) error

	// Header returns the header row of the matrix.
	Header() []string

	// Rows returns all rows of the matrix.
	Rows() [][]string

	// Data returns the entire dataset with or without the header.
	//
	// Parameters:
	//   - withHeader: Want return with header or not.
	// Returns:
	//   - If param true, return data include header.
	Data(withHeader bool) [][]string

	// Clear removes all rows from the matrix.
	Clear()

	// Preview displays the first N rows of the matrix.
	Preview(n int)

	// ToCSV exports the matrix to CSV format.
	//
	// Parameters:
	//   - withHeader: Want return with header or not.
	// Returns:
	//   - If param true, return csv data include header.
	ToCSV(withHeader bool) Output

	// ToTSV exports the matrix to TSV format.
	//
	// Parameters:
	//   - withHeader: Want return with header or not.
	// Returns:
	//   - If param true, return TSV data include header.
	ToTSV(withHeader bool) Output

	// ToYAML exports the matrix to YAML format.
	ToYAML() Output

	// ToJSON exports the matrix to JSON format.
	//
	// Parameters:
	//   - compact: Want return json data with format minified (compact) or not.
	// Returns:
	//   - If param true, return json data with format minified (compact). If param false, return json data with format pretty-printed.
	ToJSON(compact bool) Output

	// ToCustom exports the matrix to a custom format using a specified separator.
	//
	// Parameters:
	//   - withHeader: Want return with header or not.
	//   - separator: Anything separator that want to use.
	// Returns:
	//   - Data with custom format using a specified separator.
	ToCustom(withHeader bool, separator string) Output

	// AddColumn adds a new column with an empty value for all rows.
	//
	// Parameters:
	//   - key: The naming of column want to be added.
	//   - data: The value of the column.
	//
	// Returns:
	//   - An error if writing fails.
	AddColumn(key string, data ...string) error

	// AddColumns adds multiple new columns with empty values for all rows.
	//
	// Parameters:
	//   - key: The naming of columns want to be added.
	//
	// Returns:
	//   - An error if writing fails.
	AddColumns(keys ...string) error

	// AddColumnWithDefaultValue adds a column with a default value for all rows.
	//
	// Parameters:
	//   - defaultValue: Default value that want to be added on the new column.
	//   - key: The naming of column want to be added.
	//
	// Returns:
	//   - An error if writing fails.
	AddColumnWithDefaultValue(defaultValue, key string) error

	// AddColumnsWithDefaultValue adds multiple columns with a default value for all rows.
	//
	// Parameters:
	//   - defaultValue: Default value that want to be added on the new column.
	//   - keys: The naming of columns want to be added.
	//
	// Returns:
	//   - An error if writing fails.
	AddColumnsWithDefaultValue(defaultValue string, keys ...string) error

	// GetRowData retrieves a specific cell value from a row and column.
	//
	// Parameters:
	//   - index: The index of the row.
	//   - key: The naming of columns.
	//
	// Returns:
	//   - The value of the spesific index row and column.
	//   - An error if writing fails.
	GetRowData(index int, key string) (string, error)

	// UpdateRowColumn updates a specific cell value in a row and column.
	//
	// Parameters:
	//   - index: The index of the row that want to be updated.
	//   - key: The naming of columns that want to be updated.
	//
	// Returns:
	//   - An error if writing fails.
	UpdateRowColumn(index int, key string, value string) error

	// DeleteColumn removes a column from the matrix.
	//
	// Parameters:
	//   - key: The naming of columns that want to be deleted.
	//
	// Returns:
	//   - An error if writing fails.
	DeleteColumn(key string) error

	// DeleteEmptyColumns removes all empty columns from the matrix.
	DeleteEmptyColumns() error

	// ContainsValue
	ContainsValue(key string, value string) (bool, error)

	// LenColumns returns the number of columns in the matrix.
	LenColumns() int

	// LenRows returns the number of rows in the matrix.
	LenRows() int

	// DataMap returns the matrix as a slice of maps where keys are column names.
	DataMap() []map[string]string

	// Copy creates a deep copy of the matrix.
	Copy() BDataMatrix

	// Peek prints a preview of the matrix.
	Peek()
}

// New create a new BDataMatrix with the provided headers.
//
// Example usage:
//
//	// Create a new matrix with headers "ID", "Name", "Age".
//	matrix, err := New("ID", "Name", "Age")
//	if err != nil {
//	    // handle error
//	}
//
//	// Add rows.
//	_ = matrix.AddRow("1", "Alice", "30")
//	_ = matrix.AddRow("2", "Bob", "25")
//
//	// Find rows where "Name" equals "Alice" (case-insensitive).
//	query := FindRowsQuery{
//	    Column:          "Name",
//	    Operator:        OperatorEquals,
//	    CaseInsensitive: true,
//	    Values:          []string{"Alice"},
//	}
//	result, err := matrix.FindRows(query)
//	if err != nil {
//	    // handle error
//	}
//
//	// Preview the matrix.
//	matrix.Preview()
//
//	// Export as CSV (with header) and write to file.
//	csvOut := matrix.ToCSV(true)
//	_ = csvOut.Write("output.csv", 0644)
func New(keys ...string) (BDataMatrix, error) {
	if len(keys) < 1 {
		return nil, ErrEmptyHeader
	}
	t := &bDataMatrix{header: keys}
	if err := t.calculateHeaderIndex(); err != nil {
		return nil, err
	}
	return t, nil
}

// NewWithData creates a new BDataMatrix with the provided headers and initial data.
//
// Example usage:
//
//	// Define initial data rows.
//	rows := [][]string{
//	    {"1", "Alice", "30"},
//	    {"2", "Bob", "25"},
//	}
//
//	// Create a new matrix with headers and data.
//	matrix, err := NewWithData(rows, "ID", "Name", "Age")
//	if err != nil {
//	    // handle error
//	}
//
//	// Preview the matrix.
//	matrix.Preview()
//
//	// Export as JSON (compact format).
//	jsonOut := matrix.ToJSON(true, true)
//	_ = jsonOut.Write("output.json", 0644)
func NewWithData(rows [][]string, keys ...string) (BDataMatrix, error) {
	bd, err := New(keys...)
	if err != nil {
		return nil, err
	}
	if err = bd.AddRows(rows...); err != nil {
		return nil, err
	}
	return bd, nil
}

// Output defines methods for exporting matrix data.
//
// Example usage:
//
//	// Get CSV output and write to file.
//	csvOut := matrix.ToCSV(true)
//	err := csvOut.Write("output.csv", 0644)
//	if err != nil {
//	    // handle error
//	}
//
//	// Retrieve JSON output as a string.
//	jsonOut := matrix.ToJSON(true, false)
//	fmt.Println(jsonOut.String())
type Output interface {
	// Write writes the output data to a file with the given name and file mode.
	//
	// Parameters:
	//   - name: The filename to write to.
	//   - mode: The file mode (permissions) to use when writing.
	//
	// Returns:
	//   - An error if writing fails.
	Write(name string, mode os.FileMode) error

	// Bytes returns the output data as a byte slice.
	//
	// Returns:
	//   - A []byte containing the output data.
	Bytes() []byte

	// String returns the output data as a string.
	//
	// Returns:
	//   - A string representation of the output data.
	String() string
}

// ---------------------------------------------------------------------------------------------------------------------
// BDataMatrix Implementation
// ---------------------------------------------------------------------------------------------------------------------

type bDataMatrix struct {
	header      []string
	rows        [][]string
	headerIndex map[string]int
}

func (t *bDataMatrix) AddRow(values ...string) error {
	if len(values) != t.LenColumns() {
		return fmt.Errorf("row length (%d) does not match header length (%d)", len(values), t.LenColumns())
	}
	t.rows = append(t.rows, values)
	return nil
}

func (t *bDataMatrix) AddRows(rows ...[]string) error {
	for _, row := range rows {
		if err := t.AddRow(row...); err != nil {
			return err
		}
	}
	return nil
}

func (t *bDataMatrix) AddColumn(key string, data ...string) error {
	return t.AddColumnWithValue(key, data...)
}

func (t *bDataMatrix) AddColumns(keys ...string) error {
	return t.AddColumnsWithDefaultValue("", keys...)
}

func (t *bDataMatrix) AddColumnWithValue(key string, value ...string) error {
	if _, exists := t.headerIndex[key]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateHeader, key)
	}
	t.header = append(t.header, key)
	if t.LenRows() < len(value) {
		return fmt.Errorf("%w: %v", ErrRowIndexOutOfRange, t.LenRows())
	}

	if t.LenRows() > len(value) {
		for i := range value {
			t.rows[i] = append(t.rows[i], value[i])
		}
	}

	if t.LenRows() == len(value) {
		for i := range t.rows {
			t.rows[i] = append(t.rows[i], value[i])
		}
	}
	return t.calculateHeaderIndex()
}

func (t *bDataMatrix) AddColumnWithDefaultValue(defaultValue, key string) error {
	if _, exists := t.headerIndex[key]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateHeader, key)
	}
	t.header = append(t.header, key)
	for i := range t.rows {
		t.rows[i] = append(t.rows[i], defaultValue)
	}
	return t.calculateHeaderIndex()
}

func (t *bDataMatrix) AddColumnsWithDefaultValue(defaultValue string, keys ...string) error {
	for _, key := range keys {
		if err := t.AddColumnWithDefaultValue(defaultValue, key); err != nil {
			return err
		}
	}
	return nil
}

func (t *bDataMatrix) GetRowData(index int, key string) (string, error) {
	idx, exists := t.headerIndex[key]
	if !exists {
		return "", fmt.Errorf("%w: %s", ErrColumnNotFound, key)
	}
	if index < 0 || index >= t.LenRows() {
		return "", fmt.Errorf("%w: %d", ErrRowIndexOutOfRange, index)
	}
	return t.rows[index][idx], nil
}

func (t *bDataMatrix) GetRow(index int) ([]string, error) {
	if index < 0 || index >= t.LenRows() {
		return nil, fmt.Errorf("%w: %d", ErrRowIndexOutOfRange, index)
	}
	return t.rows[index], nil
}

func (t *bDataMatrix) GetRows(indexes ...int) (BDataMatrix, error) {
	rows := make([][]string, len(indexes))
	for i, index := range indexes {
		row, err := t.GetRow(index)
		if err != nil {
			return nil, err
		}
		rows[i] = row
	}
	return NewWithData(rows, t.header...)
}

func (t *bDataMatrix) GetColumn(key string) ([]string, error) {
	idx, exists := t.headerIndex[key]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, key)
	}
	column := make([]string, t.LenRows())
	for i, row := range t.rows {
		column[i] = row[idx]
	}
	return column, nil
}

func (t *bDataMatrix) GetColumns(keys ...string) (BDataMatrix, error) {
	newRows := make([][]string, t.LenRows())
	for i, row := range t.rows {
		newRow := make([]string, len(keys))
		for j, key := range keys {
			idx, exists := t.headerIndex[key]
			if !exists {
				return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, key)
			}
			newRow[j] = row[idx]
		}
		newRows[i] = newRow
	}
	return NewWithData(newRows, keys...)
}

func (t *bDataMatrix) UpdateRow(index int, values ...string) error {
	if index < 0 || index >= t.LenRows() {
		return fmt.Errorf("%w: %d", ErrRowIndexOutOfRange, index)
	}
	if len(values) != t.LenColumns() {
		return fmt.Errorf("row length (%d) does not match header length (%d)", len(values), t.LenColumns())
	}
	t.rows[index] = values
	return nil
}

func (t *bDataMatrix) UpdateRowColumn(index int, key string, value string) error {
	idx, exists := t.headerIndex[key]
	if !exists {
		return fmt.Errorf("%w: %s", ErrColumnNotFound, key)
	}
	if index < 0 || index >= t.LenRows() {
		return fmt.Errorf("%w: %d", ErrRowIndexOutOfRange, index)
	}
	t.rows[index][idx] = value
	return nil
}

func (t *bDataMatrix) DeleteRow(index int) error {
	if index < 0 || index >= t.LenRows() {
		return fmt.Errorf("%w: %d", ErrRowIndexOutOfRange, index)
	}
	t.rows = append(t.rows[:index], t.rows[index+1:]...)
	return nil
}

func (t *bDataMatrix) DeleteColumn(key string) error {
	idx, exists := t.headerIndex[key]
	if !exists {
		return fmt.Errorf("%w: %s", ErrColumnNotFound, key)
	}
	if t.LenColumns() == 1 {
		return ErrDeleteLastColumn
	}
	newHeader := append(t.header[:idx], t.header[idx+1:]...)
	newRows := make([][]string, t.LenRows())
	for i, row := range t.rows {
		newRows[i] = append(row[:idx], row[idx+1:]...)
	}
	t.header = newHeader
	t.rows = newRows
	_ = t.calculateHeaderIndex()
	return nil
}

func (t *bDataMatrix) DeleteEmptyColumns() error {
	nonEmptyColumns := make([]bool, t.LenColumns())
	for _, row := range t.rows {
		for i, val := range row {
			if strings.TrimSpace(val) != "" {
				nonEmptyColumns[i] = true
			}
		}
	}
	var newHeader []string
	for i, col := range t.header {
		if nonEmptyColumns[i] {
			newHeader = append(newHeader, col)
		}
	}
	if len(newHeader) == 0 {
		return ErrDeleteLastColumn
	}
	newRows := make([][]string, t.LenRows())
	for i, row := range t.rows {
		var newRow []string
		for j, val := range row {
			if nonEmptyColumns[j] {
				newRow = append(newRow, val)
			}
		}
		newRows[i] = newRow
	}
	t.header = newHeader
	t.rows = newRows
	_ = t.calculateHeaderIndex()
	return nil
}

// Operator defines the type of comparison for queries.
type Operator int

const (
	OperatorEquals Operator = iota + 1
	OperatorNotEquals
	OperatorContains
	OperatorStartsWith
	OperatorEndsWith
)

func (o Operator) String() string {
	v, ok := map[Operator]string{
		OperatorEquals:     "equals",
		OperatorNotEquals:  "not_equals",
		OperatorContains:   "contains",
		OperatorStartsWith: "starts_with",
		OperatorEndsWith:   "ends_with",
	}[o]
	if !ok {
		return "unknown"
	}
	return v
}

// FindRowsQuery specifies the criteria for searching rows.
//
// If both FindRowsQuery.Value and FindRowsQuery.Values present,
// FindRowsQuery.Value will be added to be one of FindRowsQuery.Values entry.
type FindRowsQuery struct {
	// Column is the header name of the column to search.
	Column string
	// Operator is the comparison operator to apply.
	Operator Operator
	// CaseInsensitive indicates whether the comparison should ignore letter case.
	CaseInsensitive bool
	// Value is a value to compare against.
	Value string
	// Values is a slice of values to compare against.
	Values []string
}

const (
	FindRowsQueryStatus_Entries       = "entries"
	FindRowsQueryStatus_MeetCondition = "meet_condition"
)

func (t *bDataMatrix) FindRowsWithHistories(query FindRowsQuery) (BDataMatrix, BDataMatrix, error) {
	cVals, err := t.GetColumn(query.Column)
	if err != nil {
		return nil, nil, err
	}
	qs, err := New(FindRowsQueryStatus_Entries, FindRowsQueryStatus_MeetCondition)
	if err != nil {
		return nil, nil, err
	}
	if query.Value != "" {
		query.Values = append(query.Values, query.Value)
	}

	matchedIndexesUnique := make(map[int]struct{})

	if query.Operator == OperatorNotEquals {
		// For each query value, record whether there is at least one row
		// that is not equal to that value.
		for _, qVal := range query.Values {
			var found bool
			for _, target := range cVals {
				if match(query.Operator, target, qVal, query.CaseInsensitive) {
					found = true
					break
				}
			}
			if err = qs.AddRow(qVal, fmt.Sprint(found)); err != nil {
				return nil, nil, err
			}
		}
		// For filtering, a row must be not equal to every query value.
		for i, target := range cVals {
			meetsAll := true
			for _, qVal := range query.Values {
				if !match(query.Operator, target, qVal, query.CaseInsensitive) {
					meetsAll = false
					break
				}
			}
			if meetsAll {
				matchedIndexesUnique[i] = struct{}{}
			}
		}
	} else {
		// For other operators, a row is selected if it meets the condition for any query value.
		for _, qVal := range query.Values {
			var found bool
			for i, target := range cVals {
				if match(query.Operator, target, qVal, query.CaseInsensitive) {
					matchedIndexesUnique[i] = struct{}{}
					found = true
				}
			}
			if err = qs.AddRow(qVal, fmt.Sprint(found)); err != nil {
				return nil, nil, err
			}
		}
	}

	if qs.LenRows() != 0 {
		if err = qs.SortByAsc(FindRowsQueryStatus_MeetCondition, FindRowsQueryStatus_Entries); err != nil {
			return nil, nil, err
		}
	}
	if len(matchedIndexesUnique) == 0 {
		return nil, nil, fmt.Errorf("%w: no rows found where column '%s' matches criteria", ErrNoRowsFound, query.Column)
	}
	var matchedIndexes []int
	for idx := range matchedIndexesUnique {
		matchedIndexes = append(matchedIndexes, idx)
	}
	nm, err := t.GetRows(matchedIndexes...)
	if err != nil {
		return nil, nil, err
	}
	return nm, qs, nil
}

func (t *bDataMatrix) FindRows(query FindRowsQuery) (BDataMatrix, error) {
	cVals, err := t.GetColumn(query.Column)
	if err != nil {
		return nil, err
	}
	if query.Value != "" {
		query.Values = append(query.Values, query.Value)
	}

	matchedIndexesUnique := make(map[int]struct{})

	if query.Operator == OperatorNotEquals {
		// For filtering, a row must be not equal to every query value.
		for i, target := range cVals {
			meetsAll := true
			for _, qVal := range query.Values {
				if !match(query.Operator, target, qVal, query.CaseInsensitive) {
					meetsAll = false
					break
				}
			}
			if meetsAll {
				matchedIndexesUnique[i] = struct{}{}
			}
		}
	} else {
		// For other operators, a row is selected if it meets the condition for any query value.
		for _, qVal := range query.Values {
			for i, target := range cVals {
				if match(query.Operator, target, qVal, query.CaseInsensitive) {
					matchedIndexesUnique[i] = struct{}{}
				}
			}
		}
	}
	if len(matchedIndexesUnique) == 0 {
		return nil, fmt.Errorf("%w: no rows found where column '%s' matches criteria", ErrNoRowsFound, query.Column)
	}
	var matchedIndexes []int
	for idx := range matchedIndexesUnique {
		matchedIndexes = append(matchedIndexes, idx)
	}
	nm, err := t.GetRows(matchedIndexes...)
	if err != nil {
		return nil, err
	}
	return nm, nil
}

func (t *bDataMatrix) sortBy(isAsc bool, keys ...string) error {
	if len(keys) == 0 {
		keys = t.header
	}
	for _, h := range keys {
		if _, exists := t.headerIndex[h]; !exists {
			return fmt.Errorf("%w: %s", ErrColumnNotFound, h)
		}
	}
	sort.SliceStable(t.rows, func(i, j int) bool {
		for _, h := range keys {
			idx := t.headerIndex[h]
			if t.rows[i][idx] != t.rows[j][idx] {
				if isAsc {
					return t.rows[i][idx] < t.rows[j][idx]
				} else {
					return t.rows[i][idx] > t.rows[j][idx]
				}

			}
		}
		return false
	})
	return nil
}

func (t *bDataMatrix) SortByDesc(keys ...string) error {
	err := t.sortBy(false, keys...)
	if err != nil {
		return err
	}
	return nil
}

func (t *bDataMatrix) SortByAsc(keys ...string) error {
	err := t.sortBy(true, keys...)
	if err != nil {
		return err
	}
	return nil
}

func (t *bDataMatrix) Header() []string {
	return t.header
}

func (t *bDataMatrix) Rows() [][]string {
	return t.rows
}

func (t *bDataMatrix) Data(withHeader bool) [][]string {
	data := make([][]string, 0, t.LenRows())
	if withHeader {
		data = make([][]string, 0, t.LenRows()+1)
		data = append(data, t.header)
	}
	data = append(data, t.rows...)
	return data
}

func (t *bDataMatrix) DataMap() []map[string]string {
	data := make([]map[string]string, t.LenRows())
	for i, row := range t.rows {
		obj := make(map[string]string)
		for j, key := range t.header {
			obj[key] = row[j]
		}
		data[i] = obj
	}
	return data
}

func (t *bDataMatrix) Copy() BDataMatrix {
	newHeader := make([]string, len(t.header))
	copy(newHeader, t.header)
	newRows := make([][]string, len(t.rows))
	for i, row := range t.rows {
		newRow := make([]string, len(row))
		copy(newRow, row)
		newRows[i] = newRow
	}
	newHeaderIndex := make(map[string]int, len(t.headerIndex))
	for key, value := range t.headerIndex {
		newHeaderIndex[key] = value
	}
	return &bDataMatrix{
		header:      newHeader,
		rows:        newRows,
		headerIndex: newHeaderIndex,
	}
}

func (t *bDataMatrix) LenColumns() int {
	return len(t.header)
}

func (t *bDataMatrix) LenRows() int {
	return len(t.rows)
}

func (t *bDataMatrix) Clear() {
	t.rows = [][]string{}
}

func (t *bDataMatrix) Peek() {
	t.Preview(5)
}

func (t *bDataMatrix) Preview(n int) {
	if n <= 0 {
		n = 10
	}
	n = min(n, t.LenRows()) // Ensure n does not exceed total rows

	// Calculate maximum width for each column relative to the first n rows.
	widths := make([]int, t.LenColumns())
	for i, h := range t.header {
		widths[i] = len(h)
	}
	for i := 0; i < n; i++ { // Only iterate over the first n rows
		for j, cell := range t.rows[i] {
			widths[j] = max(widths[j], len(cell))
		}
	}

	// Helper to print a separator line.
	printSeparator := func() {
		fmt.Print("+")
		for _, w := range widths {
			fmt.Print(strings.Repeat("-", w+2), "+")
		}
		fmt.Println()
	}

	// Helper to print a row.
	printRow := func(row []string) {
		fmt.Print("|")
		for i, cell := range row {
			fmt.Printf(" %-*s |", widths[i], cell)
		}
		fmt.Println()
	}

	printSeparator()
	printRow(t.header)
	printSeparator()
	for i := 0; i < n; i++ {
		printRow(t.rows[i])
	}
	printSeparator()

	if n < t.LenRows() {
		fmt.Printf("...and %d more rows are not shown (out of %d total).\n", t.LenRows()-n, t.LenRows())
	}
}

func (t *bDataMatrix) ToCSV(withHeader bool) Output {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.WriteAll(t.Data(withHeader)); err != nil {
		return &outputData{data: []byte(fmt.Sprintf("error writing CSV: %v", err))}
	}
	writer.Flush()
	return &outputData{data: buf.Bytes()}
}

func (t *bDataMatrix) ToTSV(withHeader bool) Output {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	writer.Comma = '\t'
	if err := writer.WriteAll(t.Data(withHeader)); err != nil {
		return &outputData{data: []byte(fmt.Sprintf("error writing TSV: %v", err))}
	}
	writer.Flush()
	return &outputData{data: buf.Bytes()}
}

func (t *bDataMatrix) ToJSON(compact bool) Output {
	var output []byte
	var err error
	if compact {
		output, err = json.Marshal(t.DataMap())
	} else {
		output, err = json.MarshalIndent(t.DataMap(), "", "  ")
	}
	if err != nil {
		return nil
	}
	return &outputData{data: output}
}

func (t *bDataMatrix) ToYAML() Output {
	output, err := yaml.Marshal(t.DataMap())
	if err != nil {
		return nil
	}
	return &outputData{data: output}
}

func (t *bDataMatrix) ToCustom(withHeader bool, separator string) Output {
	var sb strings.Builder
	rows := t.Data(withHeader)
	for i, row := range rows {
		sb.WriteString(strings.Join(row, separator))
		if i < len(rows)-1 {
			sb.WriteString("\n")
		}
	}
	return &outputData{data: []byte(sb.String())}
}

func (t *bDataMatrix) ContainsValue(key string, value string) (bool, error) {
	cValue, err := t.GetColumn(key)
	if err != nil {
		return false, ErrColumnNotFound
	}

	for _, val := range cValue {
		if strings.ContainsAny(val, value) {
			return true, nil
		}
	}
	return false, fmt.Errorf("not contains value")
}

func (t *bDataMatrix) calculateHeaderIndex() error {
	t.headerIndex = make(map[string]int)
	for i, h := range t.header {
		if _, ok := t.headerIndex[h]; !ok {
			t.headerIndex[h] = i
			continue
		}
		return fmt.Errorf("%w: %s", ErrDuplicateHeader, h)
	}
	return nil
}

// ---------------------------------------------------------------------------------------------------------------------
// Output Implementation
// ---------------------------------------------------------------------------------------------------------------------

type outputData struct {
	data []byte
}

func (o *outputData) Write(name string, mode os.FileMode) error {
	return os.WriteFile(name, o.data, mode)
}

func (o *outputData) Bytes() []byte {
	return o.data
}

func (o *outputData) String() string {
	return string(o.data)
}

// ---------------------------------------------------------------------------------------------------------------------
// Utils
// ---------------------------------------------------------------------------------------------------------------------

func match(op Operator, cVal, qVal string, caseInsensitive bool) bool {
	if caseInsensitive {
		cVal = strings.ToLower(cVal)
		qVal = strings.ToLower(qVal)
	}
	switch op {
	case OperatorEquals:
		return cVal == qVal
	case OperatorNotEquals:
		return cVal != qVal
	case OperatorContains:
		return strings.Contains(cVal, qVal)
	case OperatorStartsWith:
		return strings.HasPrefix(cVal, qVal)
	case OperatorEndsWith:
		return strings.HasSuffix(cVal, qVal)
	default:
		return false
	}
}
