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
		- (?) field types
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

### Important additions

- processing
	- *add more processors*
	- [x] average (ensemble) cycle
	- [x] arithmetic expressions
		- support arbitrary arithmetic expressions with fields/constants
- input
	- [x] (?) combine multiple files into single dataframe
        - ~~what exactly are we trying to combine?~~
        - see the io refactor bullet under [Sec. 'Quality of life'](#quality-of-life) 
	- [x] support OpenFOAM time series type inputs:
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

### Quality of life

- [x] add option to skip reading the input
- [ ] ~~specifying `input.fields` should be optional~~
	- already was optional
- [x] add option for skipping an entire pipeline
- [ ] better control over TeX graphs
	- either custom templates, or support raw TeX in config file
	- [x] support for custom templates
	- [ ] add cli command for generating/outputting default templates
	- [ ] (?) support for raw TeX in config file
- [ ] io refactor and improvements
    - [x] merge config and io class implementation
        - separating the config from the implementation is impractical, just
          make the io thing a standalone class that can read its config
          and do the work
        - watch out how we handle multiple io, because we have to preserve the
          individual configs
    - [x] remove the `directory` field from the output config
        - this should be implied when specifying the output file name, we
          should simply create the path if it doesn't exist, there's should
          be no need to specify the output directory explicitly
        - keeping the io configs the same also simplifies the implementation,
          since the class structure can remain the same
	- [x] support output to memory
		- instead of writing data to disk, store the dataframe on a 'registry'
          and make it available to other pipelines
	- [x] support generating multiple outputs in one pipeline
		- ~~add better explanation and an example use case~~
		- useful when working with series type input
        - also separated 'graphing' and 'output' into different sections
	- [x] support combining multiple input files in to one
        - not sure if this is super necessary; also this probably isn't that
          useful until resampling is implemented
    - [x] support compressed input
        - would be handy
        - [x] compressed archives
        - [x] compressed individual files
        - **dumb implementation, refactor at some point**
    - [ ] support binary input
        - not super important
- [ ] error handling cleanup and better error messages
- [ ] purge `logrus` and use the standard `log`
    - `logrus` is totally unnecessary, and propagating the logger is just annoying
- [ ] resampling support
- [ ] (?) parallelism/concurrency (at least some parts)
- [ ] the config file template generation should be automated
    - not happening any time soon
- [ ] should make the data container (currently `dataframe.DataFrame`) an interface
    - not happening any time soon
