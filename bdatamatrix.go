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
	//       - Values: Values to compare against.
	//
	// Returns:
	//   - A new BDataMatrix with matching rows.
	//   - An error if the specified column does not exist or no rows match.
	FindRows(query FindRowsQuery) (BDataMatrix, error)

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
	// Example:
	//   matrix.Preview()
	//   Output:
	//     +----+-------+-----+
	//     | ID | Name  | Age |
	//     +----+-------+-----+
	//     | 1  | Alice | 30  |
	//     | 2  | Bob   | 25  |
	//     | 3  | alice | 28  |
	//     +----+-------+-----+
	Preview()

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
	// Parameters:
	//   - withHeader: If true, represents each row as an object with header keys.
	//
	// Returns:
	//   - An Output interface representing the YAML data.
	ToYAML(withHeader bool) Output

	// ToJSON exports the matrix in JSON format.
	//
	// Parameters:
	//   - withHeader: If true, each row is represented as an object with header keys.
	//   - compact: If false, the JSON output is pretty-printed; otherwise, it is compact.
	//
	// Returns:
	//   - An Output interface representing the JSON data.
	ToJSON(withHeader, compact bool) Output

	// ToCustom exports the matrix using a custom separator.
	//
	// Parameters:
	//   - withHeader: If true, includes the header row in the output.
	//   - separator: The string to use as a separator between columns.
	//
	// Returns:
	//   - An Output interface representing the custom-formatted data.
	ToCustom(withHeader bool, separator string) Output
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
	headerIndex := make(map[string]int)
	for i, c := range keys {
		if _, exists := headerIndex[c]; exists {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateHeader, c)
		}
		headerIndex[c] = i
	}
	return &bDataMatrix{header: keys, headerIndex: headerIndex}, nil
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
	if len(values) != len(t.header) {
		return fmt.Errorf("row length (%d) does not match header length (%d)", len(values), len(t.header))
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

func (t *bDataMatrix) GetRow(index int) ([]string, error) {
	if index < 0 || index >= len(t.rows) {
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
	column := make([]string, len(t.rows))
	for i, row := range t.rows {
		column[i] = row[idx]
	}
	return column, nil
}

func (t *bDataMatrix) GetColumns(keys ...string) (BDataMatrix, error) {
	newRows := make([][]string, len(t.rows))
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
	if index < 0 || index >= len(t.rows) {
		return fmt.Errorf("%w: %d", ErrRowIndexOutOfRange, index)
	}
	if len(values) != len(t.header) {
		return fmt.Errorf("row length (%d) does not match header length (%d)", len(values), len(t.header))
	}
	t.rows[index] = values
	return nil
}

func (t *bDataMatrix) DeleteRow(index int) error {
	if index < 0 || index >= len(t.rows) {
		return fmt.Errorf("%w: %d", ErrRowIndexOutOfRange, index)
	}
	t.rows = append(t.rows[:index], t.rows[index+1:]...)
	return nil
}

// Operator defines the type of comparison for queries.
type Operator int

const (
	OperatorEquals Operator = iota
	OperatorNotEquals
	OperatorContains
	OperatorStartsWith
	OperatorEndsWith
)

// FindRowsQuery specifies the criteria for searching rows.
type FindRowsQuery struct {
	// Column is the header name of the column to search.
	Column string
	// Operator is the comparison operator to apply.
	Operator Operator
	// CaseInsensitive indicates whether the comparison should ignore letter case.
	CaseInsensitive bool
	// Values is a slice of values to compare against.
	Values []string
}

func (t *bDataMatrix) FindRows(query FindRowsQuery) (BDataMatrix, error) {
	idx, exists := t.headerIndex[query.Column]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, query.Column)
	}

	var matchedRows [][]string
	for _, row := range t.rows {
		cellValue := row[idx]
		for _, qVal := range query.Values {
			if match(query.Operator, cellValue, qVal, query.CaseInsensitive) {
				matchedRows = append(matchedRows, row)
				break
			}
		}
	}

	if len(matchedRows) == 0 {
		return nil, fmt.Errorf("%w: no rows found where column '%s' matches criteria", ErrNoRowsFound, query.Column)
	}
	return NewWithData(matchedRows, t.header...)
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
	data := make([][]string, 0, len(t.rows))
	if withHeader {
		data = make([][]string, 0, len(t.rows)+1)
		data = append(data, t.header)
	}
	data = append(data, t.rows...)
	return data
}

func (t *bDataMatrix) Clear() {
	t.rows = [][]string{}
}

func (t *bDataMatrix) Preview() {
	if len(t.header) == 0 {
		fmt.Println("No data available.")
		return
	}

	colWidths := make([]int, len(t.header))
	for i, col := range t.header {
		colWidths[i] = len(col)
	}
	for _, row := range t.rows {
		for i, cell := range row {
			if len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	printSeparator := func() {
		for _, width := range colWidths {
			fmt.Print("+", strings.Repeat("-", width+2))
		}
		fmt.Println("+")
	}

	printRow := func(cells []string) {
		for i, cell := range cells {
			fmt.Printf("| %-*s ", colWidths[i], cell)
		}
		fmt.Println("|")
	}

	printSeparator()
	printRow(t.header)
	printSeparator()
	for _, row := range t.rows {
		printRow(row)
	}
	printSeparator()
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

func (t *bDataMatrix) ToYAML(withHeader bool) Output {
	var data interface{}
	if withHeader {
		result := make([]map[string]string, 0, len(t.rows))
		for _, row := range t.rows {
			m := make(map[string]string)
			for i, header := range t.header {
				m[header] = row[i]
			}
			result = append(result, m)
		}
		data = result
	} else {
		data = t.rows
	}
	out, err := yaml.Marshal(data)
	if err != nil {
		out = []byte(fmt.Sprintf("error encoding YAML: %v", err))
	}
	return &outputData{data: out}
}

func (t *bDataMatrix) ToJSON(withHeader, compact bool) Output {
	var out []byte
	var err error

	if withHeader {
		result := make([]map[string]string, 0, len(t.rows))
		for _, row := range t.rows {
			m := make(map[string]string)
			for i, header := range t.header {
				m[header] = row[i]
			}
			result = append(result, m)
		}
		if compact {
			out, err = json.Marshal(result)
		} else {
			out, err = json.MarshalIndent(result, "", "\t")
		}
	} else {
		if compact {
			out, err = json.Marshal(t.rows)
		} else {
			out, err = json.MarshalIndent(t.rows, "", "\t")
		}
	}
	if err != nil {
		out = []byte(fmt.Sprintf("error encoding JSON: %v", err))
	}
	return &outputData{data: out}
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

func match(op Operator, cell, queryValue string, caseInsensitive bool) bool {
	if caseInsensitive {
		cell = strings.ToLower(cell)
		queryValue = strings.ToLower(queryValue)
	}

	switch op {
	case OperatorEquals:
		return cell == queryValue
	case OperatorNotEquals:
		return cell != queryValue
	case OperatorContains:
		return strings.Contains(cell, queryValue)
	case OperatorStartsWith:
		return strings.HasPrefix(cell, queryValue)
	case OperatorEndsWith:
		return strings.HasSuffix(cell, queryValue)
	default:
		return false
	}
}
