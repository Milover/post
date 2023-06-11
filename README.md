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
	- cycle ensemble average
	- basic arithmetic (add, subtract, multiply, divide by constant)
- input
	- [ ] need to support OpenFOAM series type inputs:
		```
		.
		├── 0.0
		│   └── data.csv
		├── 0.1
		│   └── data.csv
		└── ...
		```
- config stuff
	- [ ] ~~move config stuff to separate module~~
		- keeping configs in the modules they belong to - every module pretty
		  much has it's own config, the `runner` module sources the configs
		  from top level modules, and the top level module configs source
		  module specific configs as necessary
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
			  the output module
- code organization
	- [ ] add functionality for (output) file management (creation)
	- [ ] the main `Run()` function should accept mostly raw input
		(e.g. just a config file name) and probably use only some top-level
		config stuff to create output files and then pass along raw-config
		segments for further processing
