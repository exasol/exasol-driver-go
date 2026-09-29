package connection

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuoteIdentifier(t *testing.T) {
	for _, tt := range []struct {
		name      string
		input     string
		expected  string
	}{
		{"simple", "a", `"a"`},
		{"with_quote", `inner"quote`, `"inner""quote"`},
		{"multiple_quotes", `"abc"def"ghi"`, `"""abc""def""ghi"""`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			actual := quoteIdentifier(tt.input)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
