package bdatamatrix

import "errors"

var (
	// ErrEmptyHeader is returned when no header is provided.
	ErrEmptyHeader = errors.New("empty header")

	// ErrDuplicateHeader is returned when a duplicate header is encountered.
	ErrDuplicateHeader = errors.New("duplicate header")

	// ErrRowIndexOutOfRange is returned when the specified row index is invalid.
	ErrRowIndexOutOfRange = errors.New("row index out of range")

	// ErrColumnNotFound is returned when a specified column does not exist.
	ErrColumnNotFound = errors.New("column not found")

	// ErrNoRowsFound is returned when no rows match the query criteria.
	ErrNoRowsFound = errors.New("no rows found matching criteria")

	// ErrDeleteLastColumn is returned when try to delete the last column.
	ErrDeleteLastColumn = errors.New("unable to delete last column")
)
