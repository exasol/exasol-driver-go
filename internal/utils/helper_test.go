package utils

import (
	"regexp"
	"testing"

	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestBooleanHelpers(t *testing.T) {
	assert.Equal(t, 1, BoolToInt(true))
	assert.Equal(t, 0, BoolToInt(false))
	assert.True(t, *BoolToPtr(true))
	assert.False(t, *BoolToPtr(false))
}

func TestOpenFileReportsMissingFile(t *testing.T) {
	file, err := OpenFile("./.does_not_exist")

	assert.Nil(t, file)
	assert.EqualError(t, err, "E-EGOD-28: file './.does_not_exist' not found")
}

func TestParseRange(t *testing.T) {
	rangePattern := regexp.MustCompile(`^((.+?)(\d+))\.\.(\d+)$`)

	hosts, err := ParseRange(rangePattern, "node1..3")

	assert.NoError(t, err)
	assert.Equal(t, []string{"node1", "node2", "node3"}, hosts)
}

func TestParseRangeRejectsDescendingRange(t *testing.T) {
	rangePattern := regexp.MustCompile(`^((.+?)(\d+))\.\.(\d+)$`)

	hosts, err := ParseRange(rangePattern, "node3..1")

	assert.Nil(t, hosts)
	assert.ErrorIs(t, err, errors.NewInvalidHostRangeLimits("node3..1"))
}

func TestResolveHosts(t *testing.T) {
	hosts, err := ResolveHosts("node1..2,localhost")

	assert.NoError(t, err)
	assert.Equal(t, []string{"node1", "node2", "localhost"}, hosts)
}

func TestShuffleHostsPreservesMembers(t *testing.T) {
	hosts := []string{"first", "second", "third"}

	ShuffleHosts(hosts)

	assert.ElementsMatch(t, []string{"first", "second", "third"}, hosts)
}
