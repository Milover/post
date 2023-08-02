# post

A program for processing structured data files in bulk.

### TODO

- [ ] release stuff
    - [ ] setup GitHub actions/releases
    - [ ] publish on [pkg.go.dev](https://pkg.go.dev/)
- [ ] better control over TeX graphs
	- either custom templates, or support raw TeX in config file
	- [x] support for custom templates
	- [ ] add cli command for generating/outputting default templates
	- [ ] (?) support for raw TeX in config file
- [ ] error handling cleanup and better error messages
- [ ] purge `logrus` and use the standard `log`
- [ ] resampling support
- [ ] (?) parallelism/concurrency (at least some parts)
- [ ] support binary input
    - not super important
- [ ] automate config file template generation
    - not happening any time soon
- [ ] make the data container an interface
    - not happening any time soon
