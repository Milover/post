# foam-postprocess

Utilities for post-processing OpenFOAM function object data.

### Basic requirements

- [x] read JSON (or YAML?) run-config file
	- YAML might be better since most input/output objects will have a similar
	  config, so we can leverage variables and references
	- describe the input file:
		- file path
		- format
		- field names
		- field types (?)
	- describe the desired output
		- output file path
		- template file path
	- should the processing be described at all?
- [x] read DAT file and convert to `[][]string`
- [x] convert `[][]string` to a `dataframe.Dataframe`
- [x] process `dataframe.Dataframe` data
	- *processing supported, but need to add more processors*
- [x] output LaTeX graphs
	- should use templates

### Fairly important additional stuff

- processing
	- *add more processors*
	- [x] average (ensemble) cycle
	- [x] arithmetic expressions
		- support arbitrary arithmetic expressions with fields/constants
- input
	- [ ] combine multiple files into single dataframe
	- [ ] support OpenFOAM time series type inputs:
		```
		.
		├── 0.0
		│   └── data.csv
		├── 0.1
		│   └── data.csv
		└── ...
		```
- config stuff
	- [ ] ~~move config stuff to separate package~~
		- keeping configs in packages they belong to - every package pretty
		  much has it's own config, the ~~`runner`~~ `cmd` package sources
		  configs from top-level packages, which in turn source package-specific
		  configs as necessary
		- this is mostly fine, although propagating the `Logger` might become
		  an issue at some point
	- [x] add input validation
		- parts using the config should handle their own parsing/validation
		- handled through default configs and subsequent checks where necessary
- logging
	- add `logrus` logging
		- [x] start simple: replace already present stuff
		- [ ] add stuff important for debugging
			- essentially done, maybe check if we need to add logging in
			  the `output` package
- code organization
	- [x] add functionality for (output) file management (creation)
	- [x] the main `Run()` function should accept mostly raw input
		(e.g. just a config file name) and probably use only some top-level
		config stuff to create output files and then pass along raw-config
		segments for further processing
		- internal package APIs only require their own config, or their own
		  config and a `dataframe.DataFrame`, as inputs
		- moved all execution controls to `cmd` package
