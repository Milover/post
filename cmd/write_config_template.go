package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	// file name for the config template
	outFile string
)

var (
	writeConfigTemplateCmd = &cobra.Command{
		Use:   "runfile",
		Short: "Generate a run file stub",
		Long:  `Generate a run file stub`,
		Args: cobra.MatchAll(
			cobra.MaximumNArgs(1),
		),
		RunE: writeConfigTemplate,
	}
)

func init() {
	writeConfigTemplateCmd.Flags().StringVar(
		&outFile,
		"outfile",
		"post.yaml",
		"set the run file stub name",
	)
}

const (
	// FIXME: this should probably be automatically assembled
	showcaseConfig string = `# run file template
- id:                           # pipeline identifier; optional
  input:
    fields: []                  # list of field names; optional
    type:                       # one of 'dat', 'csv', 'foam-series', 'ram'
   # some example type specs; there can only be 1 input type per pipeline
    type_spec:
      file:                     # input file name; usually required
     # 'csv' specific
      header:                   # 'true' by default
      delimiter:                # ',' by default
      comment:                  # '#' by default
     # 'foam-series' specific
      directory:                # series root directory
      file:                     # series data file name
      time_name:                # 'time' by default
      format_spec:              # config for an input type reader, e.g., a 'csv'
        type: csv
        type_spec:
          header:
     # 'ram' specific
      name:                     # name of the data which will be accessed
  process:
   # some example processor specs, executed in order listed
    - type: average-cycle
      type_spec:
        n_cycles:
        time_field:             # if defined, turns on 'time-matching'
        time_precision:         # machine precision by default
    - type: filter
      type_spec:
        aggregation:            # one of 'and', 'or'; by default 'or'
        filters:
          - field:
            op:                 # one of '==', '!=', '>', '>=', '<', '<='
            value:
    - type: expression
      type_spec:
        expression:             # an arithmetic expression using constants and field names
        result:                 # name of the resulting field
  output:
   # some example specs
    - type: ram
      type_spec:
        name:                   # name of the data which will be stored
    - type: csv
      type_spec:
        file:                   # output file name
        enforce_extension:      # force correct file extension; by default 'false'
  graph:
    type:                       # only 'tex' currently
    graphs:
      - name:                   # used as a basename for all graph related files
        directory:              # output directory name, created if not present
        table_file:             # optional; needed if 'tables.table_file' is undefined
        template_directory:     # template directory; optional
        template_main:          # root template file name; optional
        template_delims:        # go template delimiters; ['__{','}__'] by default; optional
        tex_command:            # 'pdflatex' by default; optional
        axes:
          - x:
              min:
              max:
              label:            # raw TeX
            y:
              min:
              max:
              label:            # raw TeX
            width:              # optional; raw TeX, axis width option
            height:             # optional; raw TeX, axis height option
            legend_style:       # optional; raw TeX, axis legend style option
            raw_options:        # optional; raw TeX, if defined all other options are ignored
            tables:
              - x_field:
                y_field:
                col_sep:        # optional; 'comma' by default
                legend_entry:   # raw TeX
                table_file:     # optional; needed if 'graphs.table_file' is undefined
`
)

func writeConfigTemplate(cmd *cobra.Command, args []string) error {
	// validate showcase
	var configs []Config
	if err := yaml.Unmarshal([]byte(showcaseConfig), &configs); err != nil {
		panic(err)
	}
	conf, err := os.Create(outFile)
	if err != nil {
		return err
	}
	_, err = conf.Write([]byte(showcaseConfig))
	if err != nil {
		return err
	}
	return conf.Close()
}
