// Post is a program for processing structured data files in bulk
//
// Usage:
//
//	post [run file] [flags]
//	post [command]
//
// Available Commands:
//
//	completion  Generate the autocompletion script for the specified shell
//	graphfile   Generate graph file stub(s)
//	help        Help about any command
//	runfile     Generate a run file stub
//
// Flags:
//
//	    --dry-run             check runfile syntax and exit
//	-h, --help                help for post
//	    --no-graph            don't write or generate graphs
//	    --no-graph-generate   don't generate graphs
//	    --no-graph-write      don't write graph files
//	    --no-output           don't output data
//	    --no-process          don't process data
//	    --only-graphs         only write and generate graphs, skip input, processing and output
//	    --skip strings        a list of pipeline IDs to be skipped during processing
//	-v, --verbose             verbose log output
package main // import github.com/Milover/post

import "github.com/Milover/post/cmd"

func main() {
	cmd.Execute()
}
