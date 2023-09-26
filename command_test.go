package fixture

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScanLine(t *testing.T) {
	testCases := []struct {
		name   string
		line   string
		args   []string
		kwargs map[string]string
	}{
		{
			name:   "empty",
			line:   "",
			args:   nil,
			kwargs: nil,
		},
		{
			name:   "args",
			line:   "options 1 and 2 plus 3",
			args:   []string{"options", "1", "and", "2", "plus", "3"},
			kwargs: nil,
		},
		{
			name:   "kwargs",
			line:   "a=1 b=2 c=3",
			args:   nil,
			kwargs: map[string]string{"a": "1", "b": "2", "c": "3"},
		},
		{
			name:   "mixed",
			line:   "options a b and c withD=3 andE=something",
			args:   []string{"options", "a", "b", "and", "c"},
			kwargs: map[string]string{"withD": "3", "andE": "something"},
		},
	}

	commandInput := &CommandInput{}

	for i := range testCases {
		testCase := testCases[i]

		t.Run(testCase.name, func(st *testing.T) {
			commandInput.Line = testCase.line

			args, kwargs, err := commandInput.ScanLine()
			if err != nil {
				st.Fatalf("failed to ScanLine: %s", err)
			}

			assert.Equal(st, testCase.args, args)
			assert.Equal(st, testCase.kwargs, kwargs)
		})
	}
}
