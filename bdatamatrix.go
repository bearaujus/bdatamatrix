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

// BDataMatrix defines the behavior for a data matrix.
type BDataMatrix interface {
	// AddRow appends a single row to the matrix.
	//
	// Parameters:
	//   - values: A variadic list of strings representing a row.
	//
	// Returns:
	//   - An error if the number of values does not match the header length.
	AddRow(values ...string) error

	// AddRows appends multiple rows to the matrix.
	//
	// Parameters:
	//   - rows: A variadic list of rows, where each row is a slice of strings.
	//
	// Returns:
	//   - An error if any row's length does not match the header length.
	AddRows(rows ...[]string) error

	// GetRow retrieves the row at the specified index.
	//
	// Parameters:
	//   - index: The zero-based index of the row.
	//
	// Returns:
	//   - The row as a slice of strings.
	//   - An error if the index is out of range.
	GetRow(index int) ([]string, error)

	// GetRows retrieves multiple rows specified by their indexes.
	//
	// Parameters:
	//   - indexes: A variadic list of row indexes.
	//
	// Returns:
	//   - A new BDataMatrix containing only the specified rows.
	//   - An error if any index is out of range.
	GetRows(indexes ...int) (BDataMatrix, error)

	// GetColumn retrieves a column by header name.
	//
	// Parameters:
	//   - key: The header name of the column.
	//
	// Returns:
	//   - A slice of strings containing the values of the column.
	//   - An error if the column does not exist.
	GetColumn(key string) ([]string, error)

	// GetColumns returns a new BDataMatrix containing only the specified columns.
	//
	// Parameters:
	//   - keys: A variadic list of header names.
	//
	// Returns:
	//   - A new BDataMatrix whose rows contain only the values from the specified columns.
	//   - An error if any of the specified columns do not exist.
	GetColumns(keys ...string) (BDataMatrix, error)

	// UpdateRow updates the row at the specified index with new values.
	//
	// Parameters:
	//   - index: The zero-based index of the row to update.
	//   - values: A variadic list of new values for the row.
	//
	// Returns:
	//   - An error if the index is out of range or if the number of values does not match the header length.
	UpdateRow(index int, values ...string) error

	// DeleteRow removes the row at the specified index.
	//
	// Parameters:
	//   - index: The zero-based index of the row to delete.
	//
	// Returns:
	//   - An error if the index is out of range.
	DeleteRow(index int) error

	// FindRows returns a new BDataMatrix containing only the rows that match the given query.
	// It searches in the column specified by query.Column and compares each row's value using
	// query.Operator and query.CaseInsensitive. A row is included if any of the query.Values match.
	//
	// Parameters:
	//   - query: A FindRowsQuery struct with fields:
	//       - Column: Column to search in.
	//       - Operator: Comparison operator (e.g., OperatorEquals, OperatorContains).
	//       - CaseInsensitive: If true, comparison ignores letter case.
	//       - Value: Value to compare against.
	//       - Values: Values to compare against. (If both Value & Values are present, it will add Value as one of the Values)
	//
	// Returns:
	//   - A new BDataMatrix with matching rows.
	//   - A new BDataMatrix for not found value(s).
	//   - An error if the specified column does not exist or no rows match.
	FindRows(query FindRowsQuery) (BDataMatrix, BDataMatrix, error)

	// SortBy sorts the rows based on the specified header keys.
	//
	// Parameters:
	//   - keys: A variadic list of header names to sort by.
	//     If no keys are provided, the matrix is sorted by all header columns in order.
	//
	// Returns:
	//   - An error if any of the specified columns do not exist.
	SortBy(keys ...string) error

	// Header returns the header row as a slice of strings.
	//
	// Returns:
	//   - A []string representing the header.
	Header() []string

	// Rows returns all rows (excluding the header) as a two-dimensional slice of strings.
	//
	// Returns:
	//   - A [][]string containing all rows.
	Rows() [][]string

	// Data returns the full dataset as a two-dimensional slice of strings.
	//
	// Parameters:
	//   - withHeader: If true, includes the header as the first row; otherwise, returns only the data rows.
	//
	// Returns:
	//   - A [][]string where the first element is the header if withHeader is true, followed by data rows.
	//   - If withHeader is false, only the data rows are returned.
	//
	// Example usage:
	//
	//	// Get the full dataset with the header included.
	//	dataWithHeader := matrix.Data(true)
	//
	//	// Get only the data rows, excluding the header.
	//	dataWithoutHeader := matrix.Data(false)
	Data(withHeader bool) [][]string

	// Clear removes all rows from the matrix while preserving the header.
	//
	// Example:
	//   matrix.Clear()
	Clear()

	// Preview prints the matrix as a formatted table.
	//
	// Parameters:
	//   - n: Total number of entry for previewing the current matrix.
	//
	// Returns:
	//     +----+-------+-----+
	//     | ID | Name  | Age |
	//     +----+-------+-----+
	//     | 1  | Alice | 30  |
	//     | 2  | Bob   | 25  |
	//     | 3  | alice | 28  |
	//     +----+-------+-----+
	Preview(n int)

	// ToCSV exports the matrix in CSV format.
	//
	// Parameters:
	//   - withHeader: If true, includes the header row in the output.
	//
	// Returns:
	//   - An Output interface representing the CSV data.
	ToCSV(withHeader bool) Output

	// ToTSV exports the matrix in TSV (tab-separated) format.
	//
	// Parameters:
	//   - withHeader: If true, includes the header row in the output.
	//
	// Returns:
	//   - An Output interface representing the TSV data.
	ToTSV(withHeader bool) Output

	// ToYAML exports the matrix in YAML format.
	//
	// Returns:
	//   - An Output interface representing the YAML data.
	ToYAML() Output

	// ToJSON exports the matrix in JSON format.
	//
	// Parameters:
	//   - compact: If false, the JSON output is pretty-printed; otherwise, it is compact.
	//
	// Returns:
	//   - An Output interface representing the JSON data.
	ToJSON(compact bool) Output

	// ToCustom exports the matrix using a custom separator.
	//
	// Parameters:
	//   - withHeader: If true, includes the header row in the output.
	//   - separator: The string to use as a separator between columns.
	//
	// Returns:
	//   - An Output interface representing the custom-formatted data.
	ToCustom(withHeader bool, separator string) Output

	// TODO: Add docs

	AddColumn(key string) error
	AddColumns(keys ...string) error
	AddColumnWithDefaultValue(defaultValue, key string) error
	AddColumnsWithDefaultValue(defaultValue string, keys ...string) error
	GetRowData(index int, key string) (string, error)
	UpdateRowColumn(index int, key string, value string) error
	DeleteColumn(key string) error
	DeleteEmptyColumns() error
	LenColumns() int
	LenRows() int
	DataMap() []map[string]string
	Copy() BDataMatrix
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

func (t *bDataMatrix) AddColumn(key string) error {
	return t.AddColumnWithDefaultValue("", key)
}

func (t *bDataMatrix) AddColumns(keys ...string) error {
	return t.AddColumnsWithDefaultValue("", keys...)
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
		if err := t.AddColumnWithDefaultValue(key, defaultValue); err != nil {
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

func (t *bDataMatrix) FindRows(query FindRowsQuery) (BDataMatrix, BDataMatrix, error) {
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
		if err = qs.SortBy(FindRowsQueryStatus_MeetCondition, FindRowsQueryStatus_Entries); err != nil {
			return nil, nil, err
		}
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

func (t *bDataMatrix) SortBy(keys ...string) error {
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
				return t.rows[i][idx] < t.rows[j][idx]
			}
		}
		return false
	})
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
	t.Preview(10)
}

func (t *bDataMatrix) Preview(n int) {
	if n <= 0 {
		n = 10
	}
	if n > t.LenRows() {
		n = t.LenRows()
	}

	// Calculate maximum width for each column.
	widths := make([]int, t.LenColumns())
	for i, h := range t.header {
		widths[i] = len(h)
	}
	for _, row := range t.rows {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
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
