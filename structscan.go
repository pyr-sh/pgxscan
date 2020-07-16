package pgxscan

import (
	"context"
	"fmt"
	"reflect"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"github.com/pkg/errors"
)

var DefaultMapper = reflectx.NewMapperFunc("db", sqlx.NameMapper)

type Queryer interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

func Get(ctx context.Context, queryer Queryer, dest interface{}, query string, args ...interface{}) error {
	rows, err := queryer.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	return ScanStruct(rows, dest)
}

func Select(ctx context.Context, queryer Queryer, dest interface{}, query string, args ...interface{}) error {
	rows, err := queryer.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	return ScanStructs(rows, dest)
}

func SelectFlat(ctx context.Context, queryer Queryer, dest interface{}, query string, args ...interface{}) error {
	rows, err := queryer.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	return ScanFlat(rows, dest)
}

// ScanStruct scans a pgx.Rows into destination struct passed by reference based on the "db" fields tags.
// This is workaround function for pgx.Rows with single row as pgx/v4 does not allow to get row metadata
// from pgx.Row - see https://github.com/jackc/pgx/issues/627 for details.
//
// If there are no rows pgx.ErrNoRows is returned.
// If there are more than one row in the result - they are ignored.
// Function call closes rows, so caller may skip it.
func ScanStruct(r pgx.Rows, dest interface{}) error {
	defer r.Close()

	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return errors.New("dest must be a pointer to a struct, not a value")
	}
	if v.IsNil() {
		return errors.New("dest is nil pointer")
	}

	if !r.Next() {
		if err := r.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}

	columns, err := rowMetadata(r, v)
	if err != nil {
		return err
	}

	fields := DefaultMapper.TraversalsByName(v.Type(), columns)
	values := make([]interface{}, len(columns))

	err = fieldsByTraversal(v, fields, values)
	if err != nil {
		return err
	}

	return r.Scan(values...)
}

func ScanFlat(r pgx.Rows, dest interface{}) error {
	defer r.Close()

	valDest := reflect.ValueOf(dest)
	if valDest.Kind() != reflect.Ptr || valDest.Elem().Kind() != reflect.Slice {
		return errors.New("invalid input, expected a pointer to a slice")
	}

	typDest := valDest.Type()
	typSlice := typDest.Elem()
	typElem := typSlice.Elem()
	valSlice := reflect.MakeSlice(typSlice, 0, 0)

	for r.Next() {
		valRow := reflect.New(typElem)
		if err := r.Scan(valRow.Interface()); err != nil {
			return errors.Wrap(err, "failed to parse a row")
		}
		valSlice = reflect.Append(valSlice, valRow.Elem())
	}

	valDest.Elem().Set(valSlice)
	return r.Err()
}

// ScanStructs scans a pgx.Rows into destination structs list passed by reference based on the "db" fields tags
func ScanStructs(r pgx.Rows, dest interface{}) error {
	defer r.Close()

	var (
		columns []string
		err     error
	)

	destType := reflect.TypeOf(dest) // either *[]test or *[]*test
	if destType.Kind() != reflect.Ptr || destType.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("expected a pointer to a slice, got %s", destType)
	}

	sliceType := destType.Elem() // either []test or []*test
	elementType := sliceType.Elem()

	var structTypeToCreate *reflect.Type
	if elementType.Kind() == reflect.Ptr {
		x := elementType.Elem()
		structTypeToCreate = &x
	} else {
		structTypeToCreate = &elementType
	}

	resultSlice := reflect.MakeSlice(sliceType, 0, 0)

	for r.Next() {
		destVal := reflect.New(*structTypeToCreate)
		if destVal.Kind() != reflect.Ptr {
			return errors.New("must return a pointer to a new struct, not a value, to ScanStructs destination")
		}
		if destVal.IsNil() {
			return errors.New("nil pointer returned to ScanStructs destination")
		}

		if len(columns) == 0 {
			columns, err = rowMetadata(r, destVal)
			if err != nil {
				return err
			}
		}

		fields := DefaultMapper.TraversalsByName(destVal.Type(), columns)
		values := make([]interface{}, len(columns))

		err := fieldsByTraversal(destVal, fields, values)
		if err != nil {
			return err
		}

		if err := r.Scan(values...); err != nil {
			return err
		}

		// pointers are only applied directly
		if destVal.Kind() == reflect.Ptr && destVal.Elem().Kind() == elementType.Kind() {
			resultSlice = reflect.Append(resultSlice, destVal.Elem())
		} else {
			resultSlice = reflect.Append(resultSlice, destVal)
		}
	}

	reflect.ValueOf(dest).Elem().Set(resultSlice)

	return r.Err()
}

func rowMetadata(r pgx.Rows, v reflect.Value) (columns []string, err error) {
	fieldDescriptions := r.FieldDescriptions()
	columns = make([]string, len(fieldDescriptions))
	for i, fieldDescription := range fieldDescriptions {
		columns[i] = string(fieldDescription.Name)
	}

	fields := DefaultMapper.TraversalsByName(v.Type(), columns)

	// if we are not unsafe and are missing fields, return an error
	if f, err := missingFields(fields); err != nil {
		return columns, fmt.Errorf("missing column %q in dest %s", columns[f], v.Type())
	}

	return
}

func missingFields(traversals [][]int) (field int, err error) {
	for i, t := range traversals {
		if len(t) == 0 {
			return i, errors.New("missing field")
		}
	}
	return 0, nil
}

func fieldsByTraversal(v reflect.Value, traversals [][]int, values []interface{}) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return errors.New("argument is not a struct")
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			values[i] = new(interface{})
			continue
		}

		f := reflectx.FieldByIndexes(v, traversal)
		if f.Kind() == reflect.Ptr {
			values[i] = f.Interface()
		} else {
			values[i] = f.Addr().Interface()
		}
	}

	return nil
}
