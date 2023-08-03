package cmd

import (
	"io/fs"
	"os"
	"path/filepath"

	"github.com/Milover/post/internal/graph"
	"github.com/spf13/cobra"
)

var (
	// output directory for the file templates
	outDir string
)

var (
	writeGraphTemplateCmd = &cobra.Command{
		Use:   "graphfile",
		Short: "Generate graph file stub(s)",
		Long:  `Generate graph file stub(s)`,
		Args: cobra.MatchAll(
			cobra.MaximumNArgs(1),
		),
		RunE: writeGraphTemplate,
	}
)

func init() {
	writeGraphTemplateCmd.Flags().StringVar(
		&outDir,
		"outdir",
		"",
		"set the graph file stub(s) output directory",
	)
}

func writeGraphTemplate(cmd *cobra.Command, args []string) error {
	fsys := &graph.DfltTeXTemplates
	walkFn := func(path string, d fs.DirEntry, err error) error {
		// stop walking on any error, since there shouldn't be any
		if err != nil {
			return err
		}
		name := filepath.Join(outDir, path)
		if name == "" {
			return nil
		}
		if d.IsDir() {
			return os.MkdirAll(name, 0755)
		}
		body, err := fsys.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(name, body, 0666)
	}
	return fs.WalkDir(fsys, ".", walkFn)
}
