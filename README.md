# post

`post` is a program for processing structured data files in bulk.

It was originally intended as an automation tool for generating [LaTeX][latex]
graphs from `functionObject` data generated by [OpenFOAM®][openfoam] simulations,
but has since evolved such that it can be used as a general structured data
processor with optional graph generation support.

It's primary use is processing and formatting data spread over multiple files
and/or archives. The main benefit being that the entire process is defined
through one or more YAML formatted run files, hence, automating data processing
pipelines is fairly simple, while no programming is necessary.

## Installation

If [Go][golang] is installed locally, the following command will compile and
install the latest version of `post`:

```shell
$ go install github.com/Milover/post@latest
```

Precompiled binaries for Linux, Windows and Mac OS (Apple silicon) are also
available under [releases][post-release].

Finally, `post` can also be built from source, assuming [Go][golang] is
available locally, by running the following commands:

```shell
$ git clone https://github.com/Milover/post
$ cd post
$ go install
```

## CLI usage

Usage:

```
post [run file] [flags]
post [command]
```

Available Commands:

```
completion  Generate the autocompletion script for the specified shell
graphfile   Generate graph file stub(s)
help        Help about any command
runfile     Generate a run file stub
```

Flags:

```
    --dry-run             check runfile syntax and exit
-h, --help                help for post
    --no-graph            don't write or generate graphs
    --no-graph-generate   don't generate graphs
    --no-graph-write      don't write graph files
    --no-output           don't output data
    --no-process          don't process data
    --only-graphs         only write and generate graphs, skip input, processing and output
    --skip strings        a list of pipeline IDs to be skipped during processing
-v, --verbose             verbose log output
```

## Run file structure

`post` is controlled by a run file in YAML format file supplied as a CLI parameter. 
The run file consists of a list of pipelines, each defining 4 sections:
`input`, `process`, `output` and `graph`. The `input` section defines input
files and formats from which data is read; the `process` section defines
operations which are applied to the data; the `output` section defines how
the processed data will be output/stored; and the `graph` section defines
how the data will be graphed.

Even though all sections are technically optional, certain sections depend on
others, specifically, the `process` and `output` sections require an `input`
section to be defined in order to work, since 'some data' is necessary for
processing/output. The `graph` section is entirely optional and can both be
omitted, defined by itself, or as part of a pipeline.

A single pipeline has the following fields:

```yaml
- id:
  input:
    type:
    fields:
    type_spec:
  process:
    - type:
      type_spec:
  output:
    - type:
      type_spec:
  graph:
    type:
      graphs:
```

- `id`: the pipeline tag, used to reference the pipeline on the CLI; optional
- `input`: the input section
    - `type`: input type; see [Input](#input) for type descriptions
    - `fields`: field (column) names of the input data; optional
    - `type_spec`: input type specific configuration
- `process`: the process section
    - `type`: process type; see [Processing](#processing) for type descriptions
    - `type_spec`: process type specific configuration
- `output`: the output section
    - `type`: output type; see [Output](#output) for type descriptions
    - `type_spec`: output type specific configuration
- `graph`: the graph section
    - `type`: graph type; see [Graphing](#graphing) for type descriptions
    - `graphs`: a list of graph type specific graph configurations

A simple run file example is shown below.

```yaml
- input:
    type: dat
    fields: [x, y]
    type_spec:
      file: 'xy.dat'
  process:
    - type: expression
      type_spec:
        expression: '100*y'
        result: 'result'
  output:
    - type: csv
      type_spec:
        file: 'output/data.csv'
  graph:
    type: tex
    graphs:
      - name: xy.tex
        directory: output
        table_file: 'output/data.csv'
        axes:
          - x:
              min: 0
              max: 1
              label: '$x$'
            y:
              min: 0
              max: 100
              label: '$100 y$'
            tables:
              - x_field: x
                y_field: result
                legend_entry: 'result'
```

The example run file instructs `post` to do the following:

1. read data from a `DAT` formatted file `xy.dat` and rename the fields (columns)
   to `x` and `y`
2. evaluate the expression `100*y` and store the result to a field named `result`
3. output the data, now containing the fields `x`, `y` and `result` to a
   `CSV` formatted file `output/data.csv`, if the directory `output` does not
   exist, it will be created
4. generate a graph using TeX in the `output` directory, using `output/data.csv`
   as the table (data) file, with `x` as the abscissa and `result` as the ordinate

For more examples see the [examples/](examples) directory.

A generic run file stub, which can be a useful starting point, can be created
by running:

```shell
$ post runfile
```

## Input

The following is a list of available input types and their descriptions
along with their run file configuration stubs.

- `archive` reads input from an archive. The archive format is inferred from
  the file name extension. The following archive formats are supported:
  `TAR`, `TAR-GZ`, `TAR-BZIP2`, `TAR-XZ`, `ZIP`. Note that `archive` input wraps
  one or more input types, i.e., the `archive` configuration only specifies
  how to read 'some data' from an archive, the wrapped input type reads the
  actual data. Another important note is that the contents of the archive are
  stored into memory the first time it is read, so if the same archive is
  used multiple times as an input source, it will be read from disk only once,
  each subsequent read will read directly from RAM. Hence it is beneficial to
  use the `archive` input type when the data consists of a large amount of
  input files, e.g., a large `time-series`.

  ```yaml
    type: archive
    type_spec:
      file:           # file path of the archive
      format_spec:    # input type configuration, e.g., a CSV input type
  ```

- `csv` reads from a `CSV` formatted file. If the file contains a header line
  the `header` field should be set to `true` and the header column names will
  be used as the field names for the data. If no header line is present the
  `header` field must be set to `false`.

  ```yaml
    type: csv
    type_spec:
      file:           # file path of the CSV file
      header:         # determines if the CSV file has a header; default 'true'
      comment:        # character to denote comments; default '#'
      delimiter:      # character to use as the field delimiter; default ','
  ```

- `dat` reads from a white-space-separated-value file. The type and amount of
  white space between columns is irrelevant, as are leading and trailing white
  spaces, as long as the number of columns (non-white space fields) is
  consistent in each row.

  ```yaml
    type: dat
    type_spec:
      file:           # file path of the DAT file
  ```

- `multiple` is a wrapper for multiple input types. Data is read from
  each input type specified and once all inputs have been read, the data from
  each input is merged into a single data instance containing all fields
  (columns) from all inputs. The number and type of input types specified is
  arbitrary, but each input must yield data with the same number of rows.

  ```yaml
    type: multiple
    type_spec:
      format_specs:   # a list of input type configurations
  ```

- `ram` reads data from an in-memory store. For the data to be read it must
  have been stored previously, e.g., a previous `output` section defines a `ram`
  output.

  ```yaml
    type: ram
    type_spec:
      name:           # key under which the data is stored
  ```

- `time-series` reads data from a time-series of structured data files in
  the following format:

   ```
   .
   ├── 0.0
   │   ├── data_0.csv
   │   ├── data_1.dat
   │   └── ...
   ├── 0.1
   │   ├── data_0.csv
   │   ├── data_1.dat
   │   └── ...
   └── ...
   ```

  where each `data_*.*` file contains the data in some format at the moment in
  time specified by the directory name.
  Each series dataset must be output into a different file, i.e., the
  `data_0.csv` files contain one dataset, `data_1.dat` another one, and so on.

  ```yaml
    type: time-series
    type_spec:
      file:           # file name (base only) of the time-series data files
      directory:      # path to the root directory of the time-series
      time_name:      # the time field name; default is 'time'
      format_spec:    # input type configuration, e.g., a CSV input type
  ```

## Processing

The following is a list of available processor types and their descriptions
along with their run file configuration stubs.

- `average-cycle` mutates the data by computing the enesemble average of a cycle
  for all numeric fields. The ensemble average is computed as:

  ```
  Φ(ωt) = 1/N Σ ϕ[ω(t+j)T], j = 0...N-1
  ```

  where `ϕ` is the slice of values to be averaged, `ω` the angular velocity,
  `t` the time and `T` the period.

  The resulting data will contain the cycle average of all numeric fields and a
  time field (named `time`), containing times for each row of cycle average
  data, in the range (0, T]. The time field will be the last field (column),
  while the order of the other fields is preserved.

  Time matching can be optionally specified, as well as the match precision,
  by setting `time_field` and `time_precision` respectively in the configuration.
  This checks whether the time (step) is uniform and whether there is a
  mismatch between the expected time of the averaged value, as per the number
  of cycles defined in the configuration and the supplied data, and the read time.
  The read time is the one read from the field named `time_field`.
  Note that in this case the output time field will be named after `time_field`,
  i.e., the time field name will remain unchanged.

  ```yaml
    type: average-cycle
    type_spec:
      n_cycles:       # number of cycles to average over
      time_field:     # time field name; optional
      time_precision: # time-matching precision; optional
  ```

- `expression` evaluates an arithmetic expression and appends the resulting
  field (column) to the data. The expression operands can be scalar values or
  fields (columns) present in the data, which are referenced by their names.
  Note that at least one of the operands must be a field present in the data.

  Each operation involving a field is applied element-wise. The following
  arithmetic operations are supported: `+` `-` `*` `/` `**`

  ```yaml
    type: expression
    type_spec:
      expression:     # an arithmetic expression
      result:         # field name of the resulting field
  ```

- `filter` mutates the data by applying a set of row filters as defined
  in the configuration. The filter behaviour is described by providing
  the field name `field` to which the filter is applied, the comparison
  operator `op` and a comparison value `value`. Rows satisfying the comparison
  are kept, while others are discarded. The following comparison operators
  are supported: `==` `!=` `>` `>=` `<` `<=`

  All defined filters are applied at the same time. The way in which they
  are aggregated is controlled by setting the `aggregation` field in
  the configuration, `and` and `or` aggregation modes are available.
  The `or` mode is the default if the `aggregation` field is unset.

  ```yaml
    type: filter
    type_spec:
      aggregation:    # aggregration mode; defaults to 'or'
      filters:
        - field:      # field name to which the filter is applied
          op:         # filtering operation
          value:      # comparison value
  ```

- `resample` mutates the data by linearly interpolating all numeric fields,
  such that the resulting fields have `n_points` values, at uniformly
  distributed values of the field `x_field`.
  If `x_field` is not set, a uniform resampling is performed, i.e., as if
  the values of each field were given at a uniformly distributed x,
  where x ∈ [0,1].
  The first and last values of a field are preserved in the resampled field.

  ```yaml
    type: resample
    type_spec:
      n_points:       # number of resampling points
      x_field:        # field name of the independent variable; optional
  ```

- `select` mutates the data by selecting fields (extracting columns)
  specified by `fields` which is a list of field names.

  ```yaml
    type: select
    type_spec:
      fields:         # a list of field names
  ```

## Output

The following is a list of available output types and their descriptions
along with their run file configuration stubs.

- `csv` writes `CSV` formatted data to a file. If `header` is set to `true`
  the file will contain a header line with the field names as the column names.
  Note that, if necessary, directories will be created so as to ensure that
  `file` specifies a valid path.

  ```yaml
    type: csv
    type_spec:
      file:           # file path of the CSV file
      header:         # determines if the CSV file has a header; default 'true'
      comment:        # character to denote comments; default '#'
      delimiter:      # character to use as the field delimiter; default ','
  ```

- `ram` stores data in an in-memory store. Once data is stored, any subsequent
  `ram` input type can access the data.

  ```yaml
    type: ram
    type_spec:
      name:           # key under which the data is stored
  ```

## Graphing

Only TeX graphing, via `tikz` and `pgfplots`, is supported currently. Hence
for the graph generation to work, TeX needs to be installed along with any
dependent packages.

Graphing consists of two steps: generating TeX graph files from templates, and
generating the graphs from TeX files. To see the default template files run:

```shell
$ post graphfile --outdir=templates
```

The templates can be user supplied by setting `template_directory` and
`template_main` (if necessary) in the run file configuration. The templates
use [Go][golang] template syntax, see the [package documentation][godoc-text-template]
for more information.

A `tex` graph configuration stub is given below, note that several fields expect
raw TeX as input.

```yaml
type: tex
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
```


[godoc-text-template]: https://pkg.go.dev/text/template
[golang]: https://go.dev
[latex]: https://www.latex-project.org/
[openfoam]: https://www.openfoam.com
[post-release]: https://github.com/Milover/post/releases
