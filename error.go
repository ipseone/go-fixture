package fixture

import "fmt"

type RecordError struct {
	Table string
	Key   string
	Field string
	Err   error
}

func (e *RecordError) Error() string {
	return fmt.Sprintf("table %s, key %s, field %s: %s", e.Table, e.Key, e.Field, e.Err)
}
