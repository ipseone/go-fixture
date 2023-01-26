package fixture

import (
	"fmt"

	"github.com/oklog/ulid/v2"
)

// ULID is meant to be used with (*Fixture).GetField,
// and will panic if the incoming err is not nil. E.g.:
//     ULID(f.GetField("users", "1", "id"))
func ULID(v interface{}, err error) ulid.ULID {
	if err != nil {
		panic(fmt.Errorf("fixture.ULID: %w", err))
	}

	return ulid.ULID(v.([16]uint8))
}
