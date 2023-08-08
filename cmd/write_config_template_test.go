package cmd

import (
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

// writeConfigTemplate command tests
type writeConfigTemplateTest struct {
	Name  string
	Error error
}

var writeConfigTemplateTests = []writeConfigTemplateTest{
	{
		Name:  "good",
		Error: nil,
	},
}

func TestWriteConfigTemplate(t *testing.T) {
	for _, tt := range writeConfigTemplateTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			tmp := t.TempDir()
			outFile = filepath.Join(tmp, "post.yaml")

			err := writeConfigTemplate(&cobra.Command{}, []string{})
			assert.Equal(tt.Error, err)
		})
	}
}
