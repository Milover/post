package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	// FIXME: this should probably be automatically assembled
	showcaseConfig string = `# run file template
- input:
    file:                       # input file name; unused if 'series_spec' is defined
    fields: []                  # list of field names
    format:                     # 'dat' or 'csv'
    format_spec:
      # 'csv' spec; 'dat' doesn't require a spec
      has_header:               # 'true' by default
      delimiter:                # ',' by default
      comment:                  # '#' by default
    # should only be defined if input is an OpenFOAM series
    series_spec:
      series_directory:         # root series directory
      series_file:              # series data file name
      series_time_name:         # generateed time field name; 'time' by default
  process:
    - type: average-cycle
      type_spec:
        n_cycles:
        time_field:             # if defined, turns on 'time-matching'
        time_precision:         # machine precision by default
    - type: filter
      type_spec:
        aggregation:            # one of 'and', 'or'
        filters:
          - field:
            op:                 # one of '==', '!=', '>', '>=', '<', '<='
            value:
    - type: expression
      type_spec:
        expression:             # an arithmetic expression using constants and field names
        result:                 # name of the resulting field
  output:
    directory:                  # output directory name, created if not present
    table_file:                 # output file name
    graphs:
      - name:                   # used as a basename for all graph related files
        axes:
          - x:
              min:
              max:
              label:            # raw TeX
            y:
              min:
              max:
              label:
            tables:
              - x_field:
                y_field:
                legend_entry:   # raw TeX
`
)

func writeConfigTemplate(cmd *cobra.Command, args []string) error {
	// validate showcase
	var configs []Config
	if err := yaml.Unmarshal([]byte(showcaseConfig), &configs); err != nil {
		panic(err)
	}
	conf, err := os.Create(configFile)
	if err != nil {
		return err
	}
	_, err = conf.Write([]byte(showcaseConfig))
	if err != nil {
		return err
	}
	return conf.Close()
}