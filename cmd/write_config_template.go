package cmd

import (
	"fmt"
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
	configStub string = `# run file template
- id:                           # optional; pipeline identifier
  input:
    fields: []                  # optional; list of field names
    type:                       # one of: 'dat', 'csv', 'time-series', 'ram', 'archive', 'multiple'
   # some example type specs; there can only be 1 input type per pipeline
    type_spec:
     # 'archive' example
      file:                     # input archive file name; supports .tar, .tgz, .txz, .tbz, .zip
      clear_after_read:         # clear memory after reading; 'false' by default
      format_spec:              # config for an input reader, e.g., a 'csv'
        type: csv
        type_spec:
          header:
     # 'csv' example
      file:                     # input file name; usually required
      header:                   # optional; 'true' by default
      delimiter:                # optional; ',' by default
      comment:                  # optional; '#' by default
     # 'dat' example
      file:                     # input file name; usually required
     # 'multiple' example
      format_specs:              # configs for multiple input readers, e.g., 'csv' and 'dat'
        - type: csv
          type_spec:
            file:
            header:
        - type: dat
          type_spec:
            file:
     # 'ram' example
      name:                     # name of the data which will be accessed
      clear_after_read:         # clear memory after reading; 'false' by default
     # 'time-series' example
      directory:                # series root directory
      file:                     # series data file name
      time_name:                # 'time' by default
      format_spec:              # config for an input type reader, e.g., a 'csv'
        type: csv
        type_spec:
          header:
  process:
   # some example processor specs, executed in order listed
    - type: assert-equal
      type_spec:
        fields:                 # field names for which to assert equality
        precision:              # optional; machine precision by default
    - type: average-cycle
      type_spec:
        n_cycles:
        time_field:             # if defined, turns on 'time-matching'
        time_precision:         # optional; machine precision by default
    - type: bin
      type_spec:
        n_bins:                 # number of bins into which the data is divided
    - type: expression
      type_spec:
        expression:             # an arithmetic expression using constants and field names
        result:                 # name of the resulting field
    - type: filter
      type_spec:
        aggregation:            # one of 'and', 'or'; 'or' by default
        filters:
          - field:
            op:                 # one of '==', '!=', '>', '>=', '<', '<='
            value:
    - type: regexp-rename
      type_spec:
        src:                    # regular expression to use in matching
        repl:                   # replacement string
    - type: rename
      type_spec:
        fields:                 # map of old-to-new name key-value pairs
    - type: resample
      type_spec:
        n_points:               # number of resampling data points
        x_field:                # optional; indepentent variable field name
    - type: select
      type_spec:
        fields:                 # list of field (column) names to extract
        remove:                 # remove/keep selected fields; 'false' by default
    - type: sort
      type_spec:
        - field:                # field by which to sort
          descending:           # sort in descending order; 'false' by default
        - field:
          descending:
  output:
   # some example specs
    - type: ram
      type_spec:
        name:                   # key name under which data will be stored
        clear_after_read:       # clear memory after reading; 'false' by default
    - type: csv
      type_spec:
        file:                   # output file name
        enforce_extension:      # optional; force correct file extension, by default 'false'
  graph:
    type:                       # only 'tex' currently
    graphs:
      - name:                   # used as a basename for all graph related files
        directory:              # optional; output directory name, created if not present
        table_file:             # optional; needed if 'tables.table_file' is undefined
        template_directory:     # optional; template directory
        template_main:          # optional; root template file name
        template_delims:        # optional; go template delimiters; ['__{','}__'] by default
        tex_command:            # optional; 'pdflatex' by default
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
                legend_entry:   # raw TeX
                col_sep:        # optional; 'comma' by default
                table_file:     # optional; needed if 'graphs.table_file' is undefined
`
)

func writeConfigTemplate(cmd *cobra.Command, args []string) error {
	// validate stub
	var configs []Config
	if err := yaml.Unmarshal([]byte(configStub), &configs); err != nil {
		return fmt.Errorf("error creating runfile stub: %w", err)
	}
	conf, err := os.Create(outFile)
	if err != nil {
		return fmt.Errorf("error creating runfile stub: %w", err)
	}
	_, err = conf.Write([]byte(configStub))
	if err != nil {
		return fmt.Errorf("error creating runfile stub: %w", err)
	}
	return conf.Close()
}
