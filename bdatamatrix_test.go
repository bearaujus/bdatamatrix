package bdatamatrix

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestNew tests the New function.
func TestNew(t *testing.T) {
	// Valid new matrix
	matrix, err := New("ID", "Name", "Age")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(matrix.Header()) != 3 {
		t.Fatalf("expected header length 3, got %d", len(matrix.Header()))
	}
	// Test empty header error.
	_, err = New()
	if err == nil {
		t.Fatal("expected error for empty header, got nil")
	}
	// Test duplicate header error.
	_, err = New("ID", "Name", "ID")
	if err == nil {
		t.Fatal("expected error for duplicate header, got nil")
	}
}

// TestNewWithData tests NewWithData.
func TestNewWithData(t *testing.T) {
	rows := [][]string{
		{"1", "Alice", "30"},
		{"2", "Bob", "25"},
	}
	matrix, err := NewWithData(rows, "ID", "Name", "Age")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(matrix.Rows()) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(matrix.Rows()))
	}
}

// TestAddRow tests AddRow.
func TestAddRow(t *testing.T) {
	matrix, _ := New("ID", "Name")
	err := matrix.AddRow("1", "Alice")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	// Test row length mismatch.
	err = matrix.AddRow("2")
	if err == nil {
		t.Fatal("expected error due to row length mismatch, got nil")
	}
}

// TestAddRows tests AddRows.
func TestAddRows(t *testing.T) {
	matrix, _ := New("ID", "Name")
	rows := [][]string{
		{"1", "Alice"},
		{"2", "Bob"},
	}
	err := matrix.AddRows(rows...)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(matrix.Rows()) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(matrix.Rows()))
	}
}

// TestGetRow tests GetRow.
func TestGetRow(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	row, err := matrix.GetRow(0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if row[0] != "1" || row[1] != "Alice" {
		t.Fatal("unexpected row content")
	}
	_, err = matrix.GetRow(1)
	if err == nil {
		t.Fatal("expected error for out-of-range index, got nil")
	}
}

// TestGetRows tests GetRows.
func TestGetRows(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	matrix.AddRow("2", "Bob")
	subMatrix, err := matrix.GetRows(0, 1)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(subMatrix.Rows()) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(subMatrix.Rows()))
	}
	// Test error for out-of-range index.
	_, err = matrix.GetRows(0, 2)
	if err == nil {
		t.Fatal("expected error for out-of-range index, got nil")
	}
}

// TestGetColumn tests GetColumn.
func TestGetColumn(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	col, err := matrix.GetColumn("Name")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(col) != 1 || col[0] != "Alice" {
		t.Fatal("unexpected column data")
	}
	_, err = matrix.GetColumn("Age")
	if err == nil {
		t.Fatal("expected error for non-existent column, got nil")
	}
}

// TestGetColumns tests GetColumns.
func TestGetColumns(t *testing.T) {
	matrix, _ := New("ID", "Name", "Age")
	matrix.AddRow("1", "Alice", "30")
	matrix.AddRow("2", "Bob", "25")
	subMatrix, err := matrix.GetColumns("Name", "Age")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(subMatrix.Header()) != 2 {
		t.Fatal("expected header length 2")
	}
	_, err = matrix.GetColumns("Name", "Gender")
	if err == nil {
		t.Fatal("expected error for non-existent column, got nil")
	}
}

// TestUpdateRow tests UpdateRow.
func TestUpdateRow(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	err := matrix.UpdateRow(0, "1", "Alicia")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	row, _ := matrix.GetRow(0)
	if row[1] != "Alicia" {
		t.Fatalf("expected updated row value 'Alicia', got %s", row[1])
	}
	// Test invalid index.
	err = matrix.UpdateRow(1, "2", "Bob")
	if err == nil {
		t.Fatal("expected error for invalid index, got nil")
	}
}

// TestDeleteRow tests DeleteRow.
func TestDeleteRow(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	matrix.AddRow("2", "Bob")
	err := matrix.DeleteRow(0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(matrix.Rows()) != 1 {
		t.Fatal("expected 1 row after deletion")
	}
	// Test invalid index.
	err = matrix.DeleteRow(5)
	if err == nil {
		t.Fatal("expected error for invalid index, got nil")
	}
}

// TestFindRows tests FindRows.
func TestFindRows(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	matrix.AddRow("2", "Bob")
	matrix.AddRow("3", "alice")
	query := FindRowsQuery{
		Column:          "Name",
		Operator:        OperatorEquals,
		CaseInsensitive: true,
		Values:          []string{"Alice"},
	}
	subMatrix, err := matrix.FindRows(query)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(subMatrix.Rows()) != 2 {
		t.Fatalf("expected 2 rows matching query, got %d", len(subMatrix.Rows()))
	}
	// Test no match.
	query.Values = []string{"NonExistent"}
	_, err = matrix.FindRows(query)
	if err == nil {
		t.Fatal("expected error when no rows match, got nil")
	}

	// Test operator not equals.
	queryNotEquals := FindRowsQuery{
		Column:          "Name",
		Operator:        OperatorNotEquals,
		CaseInsensitive: true,
		Values:          []string{"Alice"},
	}
	subMatrix, err = matrix.FindRows(queryNotEquals)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(subMatrix.Rows()) != 1 {
		t.Fatalf("expected 1 rows matching query, got %d", len(subMatrix.Rows()))
	}

	// Test search one value.
	queryValue := FindRowsQuery{
		Column:          "Name",
		Operator:        OperatorEquals,
		CaseInsensitive: true,
		Value:           "Alice",
	}
	subMatrix, err = matrix.FindRows(queryValue)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(subMatrix.Rows()) != 2 {
		t.Fatalf("expected 2 rows matching query, got %d", len(subMatrix.Rows()))
	}

	// Test not match column.
	queryNotMatchColumn := FindRowsQuery{
		Column:          "Age",
		Operator:        OperatorEquals,
		CaseInsensitive: true,
		Value:           "Alice",
	}
	_, err = matrix.FindRows(queryNotMatchColumn)
	if err == nil {
		t.Fatalf("expected error, doesn't have match column got %v", err)
	}

}

func Test_FindRowsWithHistories(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	matrix.AddRow("2", "Bob")
	matrix.AddRow("3", "alice")
	// Test operator equals.
	query := FindRowsQuery{
		Column:          "Name",
		Operator:        OperatorEquals,
		CaseInsensitive: true,
		Values:          []string{"Alice"},
	}
	subMatrix, subMatrix2, err := matrix.FindRowsWithHistories(query)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(subMatrix.Rows()) != 2 {
		t.Fatalf("expected 1 rows matching query, got %d", len(subMatrix.Rows()))
	}

	if len(subMatrix2.Rows()) != 1 {
		t.Fatalf("expected 1 rows matching query, got %d", len(subMatrix.Rows()))
	}

	// Test operator not equals.
	queryNotEquals := FindRowsQuery{
		Column:          "Name",
		Operator:        OperatorNotEquals,
		CaseInsensitive: true,
		Values:          []string{"Alice"},
	}
	subMatrixNotEquals, _, err := matrix.FindRowsWithHistories(queryNotEquals)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(subMatrixNotEquals.Rows()) != 1 {
		t.Fatalf("expected 1 rows matching query, got %d", len(subMatrixNotEquals.Rows()))
	}

	// Test not match values.
	queryNotMatch := FindRowsQuery{
		Column:          "Name",
		Operator:        OperatorEquals,
		CaseInsensitive: true,
		Values:          []string{"Bram"},
	}
	_, _, err = matrix.FindRowsWithHistories(queryNotMatch)
	if err == nil {
		t.Fatalf("expected error, because doesn't have matches value got %v", err)
	}

	// Test not match column.
	queryNotMatchColumn := FindRowsQuery{
		Column:          "Age",
		Operator:        OperatorEquals,
		CaseInsensitive: true,
		Values:          []string{"Bram"},
	}
	_, _, err = matrix.FindRowsWithHistories(queryNotMatchColumn)
	if err == nil {
		t.Fatalf("expected error, because doesn't have matches column got %v", err)
	}

	// Test
	queryValue := FindRowsQuery{
		Column:          "Name",
		Operator:        OperatorEquals,
		CaseInsensitive: true,
		Value:           "Alice",
	}
	subMatrixValue, _, err := matrix.FindRowsWithHistories(queryValue)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(subMatrixValue.Rows()) != 2 {
		t.Fatalf("expected 2 rows matching query, got %d", len(subMatrixNotEquals.Rows()))
	}

}

// TestSortByDesc tests SortByDesc.
func TestSortByDesc(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("2", "Bob")
	matrix.AddRow("1", "Alice")
	err := matrix.SortByDesc("ID")
	if err != nil {
		t.Fatalf("expected error, got %v", err)
	}
	row, _ := matrix.GetRow(0)
	if row[0] != "2" {
		t.Fatalf("expected first row ID to be 1 after sorting, got %s", row[0])
	}
	// Test sorting by non-existent column.
	err = matrix.SortByDesc("Age")
	if err == nil {
		t.Fatal("expected error when sorting by non-existent column, got nil")
	}
}

func TestSortByAsc(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("2", "Bob")
	matrix.AddRow("1", "Alice")
	err := matrix.SortByAsc("ID")
	if err != nil {
		t.Fatalf("expected error, got %v", err)
	}
	row, _ := matrix.GetRow(0)
	if row[0] != "1" {
		t.Fatalf("expected first row ID to be 1 after sorting, got %s", row[0])
	}
	// Test sorting by non-existent column.
	err = matrix.SortByAsc("Age")
	if err == nil {
		t.Fatal("expected error when sorting by non-existent column, got nil")
	}
}

// TestHeaderRowsData tests Header, Rows, and Data.
func TestHeaderRowsData(t *testing.T) {
	matrix, _ := New("ID", "Name", "Age")
	matrix.AddRow("1", "Alice", "30")
	matrix.AddRow("2", "Bob", "25")
	header := matrix.Header()
	if len(header) != 3 {
		t.Fatal("expected header length 3")
	}
	rows := matrix.Rows()
	if len(rows) != 2 {
		t.Fatal("expected 2 rows")
	}
	dataWithHeader := matrix.Data(true)
	if len(dataWithHeader) != 3 {
		t.Fatalf("expected data with header to have 3 rows, got %d", len(dataWithHeader))
	}
	dataWithoutHeader := matrix.Data(false)
	if len(dataWithoutHeader) != 2 {
		t.Fatalf("expected data without header to have 2 rows, got %d", len(dataWithoutHeader))
	}
}

// TestClear tests Clear.
func TestClear(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	matrix.Clear()
	if len(matrix.Rows()) != 0 {
		t.Fatal("expected no rows after clear")
	}
}

// TestPreview tests Preview by capturing stdout.
func TestPreview(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	matrix.AddRow("2", "Bob")

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	matrix.Peek()

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stdout = oldStdout

	output := buf.String()
	if !strings.Contains(output, "ID") || !strings.Contains(output, "Alice") {
		t.Fatal("preview output missing expected content")
	}
}

// TestToCSV tests ToCSV.
func TestToCSV(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	output := matrix.ToCSV(true)
	csvData := output.String()
	if !strings.Contains(csvData, "ID") || !strings.Contains(csvData, "Alice") {
		t.Fatal("CSV output missing expected content")
	}
	// Test CSV without header.
	output = matrix.ToCSV(false)
	csvData = output.String()
	if strings.Contains(csvData, "ID") {
		t.Fatal("CSV output should not contain header when withHeader is false")
	}
}

// TestToTSV tests ToTSV.
func TestToTSV(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	output := matrix.ToTSV(true)
	tsvData := output.String()
	if !strings.Contains(tsvData, "ID") || !strings.Contains(tsvData, "Alice") {
		t.Fatal("TSV output missing expected content")
	}
}

// TestToYAML tests ToYAML.
func TestToYAML(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	output := matrix.ToYAML()
	yamlData := output.String()
	var result []map[string]string
	err := yaml.Unmarshal([]byte(yamlData), &result)
	if err != nil {
		t.Fatalf("failed to unmarshal YAML: %v", err)
	}
	if len(result) != 1 {
		t.Fatal("expected one YAML object")
	}
}

// TestToJSON tests ToJSON.
func TestToJSON(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	output := matrix.ToJSON(false)
	jsonData := output.String()
	var result []map[string]string
	err := json.Unmarshal([]byte(jsonData), &result)
	if err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}
	if len(result) != 1 {
		t.Fatal("expected one JSON object")
	}
	// Test compact JSON output.
	output = matrix.ToJSON(true)
	jsonData = output.String()
	if strings.Contains(jsonData, "\n") {
		t.Fatal("expected compact JSON without newlines")
	}
}

// TestToCustom tests ToCustom.
func TestToCustom(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	output := matrix.ToCustom(true, " | ")
	customData := output.String()
	if !strings.Contains(customData, "ID") || !strings.Contains(customData, "Alice") {
		t.Fatal("custom output missing expected content")
	}
}

func TestAddColumn(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	matrix.AddRow("2", "Bram")
	matrix.AddRow("3", "John")
	err := matrix.AddColumn("Age", "20")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = matrix.AddColumn("Gender", "Woman", "Man", "Man")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = matrix.AddColumn("Age", "20")
	if err == nil {
		t.Fatalf("expected error duplicate header, got %v", err)
	}

	err = matrix.AddColumn("Class", "Science", "Math", "Biology", "Physic")
	if err == nil {
		t.Fatalf("expected error the length value more than length data, got %v", err)
	}
}

func TestAddColumns(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	matrix.AddColumns("Age", "Gender")

	_, err := matrix.GetColumns("Age", "Gender")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = matrix.AddColumns("Age", "Gender")
	if err == nil {
		t.Fatalf("expected error because have duplicate header %v", err)
	}
}

func TestGetRowData(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	_, err := matrix.GetRowData(0, "Name")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	_, err = matrix.GetRowData(0, "Age")
	if err == nil {
		t.Fatalf("expected error, because doesn't have column age. got %v", err)
	}

	_, err = matrix.GetRowData(1, "Name")
	if err == nil {
		t.Fatalf("expected error, because the row data only have 1 on index 0. got %v", err)
	}
}

func TestUpdateRowColumn(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	err := matrix.UpdateRowColumn(0, "Name", "Bram")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = matrix.UpdateRowColumn(0, "Age", "Bram")
	if err == nil {
		t.Fatalf("expected error, because doesn't have column Age %v", err)
	}

	err = matrix.UpdateRowColumn(1, "Name", "Bram")
	if err == nil {
		t.Fatalf("expected error, because doesn't have row index 1 %v", err)
	}
}

func TestDeleteColumn(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	err := matrix.DeleteColumn("Name")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = matrix.DeleteColumn("Age")
	if err == nil {
		t.Fatalf("expected error, because doesn't have column Age. got %v", err)
	}

	err = matrix.DeleteColumn("ID")
	if err == nil {
		t.Fatalf("expected error, because can't delete if the column have only 1. got %v", err)
	}
}

func TestDeleteEmptyColumns(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	err := matrix.DeleteEmptyColumns()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCopy(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")
	matrix.Copy()
}

func Test_bDataMatrix_ContainsValue(t *testing.T) {
	matrix, _ := New("ID", "Name")
	matrix.AddRow("1", "Alice")

	_, err := matrix.ContainsValue("Name", "alice")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	_, err = matrix.ContainsValue("Name", "bram")
	if err == nil {
		t.Fatalf("expected error, not found value bram %v", err)
	}

	_, err = matrix.ContainsValue("Class", "bram")
	if err == nil {
		t.Fatalf("expected error, not found key column %v", err)
	}
}
