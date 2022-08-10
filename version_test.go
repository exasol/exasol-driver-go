package exasol

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"os"
	"testing"
)

type projectKeeper struct {
	Version string `yaml:"version"`
}

func TestVersionIsUpToDate(t *testing.T) {
	yamlFile, err := os.ReadFile(".project-keeper.yml")
	assert.NoError(t, err)

	keeperContent := &projectKeeper{}

	err = yaml.Unmarshal(yamlFile, keeperContent)
	assert.NoError(t, err)

	assert.Equal(t, fmt.Sprintf("v%s", keeperContent.Version), driverVersion)

}
