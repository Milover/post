package cmd

import (
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/Milover/post/internal/graph"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

// writeGraphTemplate command tests
type writeGraphTemplateTest struct {
	Name  string
	Error error
}

var writeGraphTemplateTests = []writeGraphTemplateTest{
	{
		Name:  "good",
		Error: nil,
	},
}

func TestWriteGraphTemplate(t *testing.T) {
	for _, tt := range writeGraphTemplateTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			outDir = t.TempDir()
			err := writeGraphTemplate(&cobra.Command{}, []string{})
			assert.Equal(tt.Error, err)

			// compare files
			walkFn := func(list *[]string) fs.WalkDirFunc {
				return func(path string, d fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					*list = append(*list, path)
					return nil
				}
			}
			expected, actual := []string{}, []string{}
			err = fs.WalkDir(graph.DfltTeXTemplates, ".", walkFn(&expected))
			assert.Nil(err)
			err = fs.WalkDir(os.DirFS(outDir), ".", walkFn(&actual))
			assert.Nil(err)
			for i := range actual {
				a := &actual[i]
				*a = strings.TrimPrefix(*a, outDir+string(os.PathSeparator))
			}
			assert.Equal(expected, actual)
		})
	}
}
