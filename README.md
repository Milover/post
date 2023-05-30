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
- [ ] process `dataframe.Dataframe` data
- [x] output LaTeX graphs
	- should use templates

### Fairly important additional stuff

- config stuff
	- [ ] move config stuff to separate module
	- [ ] add input validation
		- parts using the config should handle their own parsing/validation
- logging
	- add `logrus` logging
		- [ ] start simple: replace already present stuff
		- [ ] add stuff important for debugging
- code organization
	- [ ] add functionality for (output) file management (creation)
	- [ ] the main `Run()` function should accept mostly raw input
		(e.g. just a config file name) and probably use only some top-level
		config stuff to create output files and then pass along raw-config
		segments for further processing
