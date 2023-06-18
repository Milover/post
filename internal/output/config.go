package output

type Config struct {
	// Directory is an output directory for all data. If it is an empty string,
	// the current working directory is used. The path is created recursively
	// if it does not exist.
	Directory string `yaml:"directory"`
	// TableFile is the file in which the CSV-fromatted dataframe.DataFrame
	// will be written.
	TableFile string `yaml:"table_file"`
	// TODO: This should be a *yaml.Node because we might not be using TeX,
	// and even if we are, the input needs to be validated.
	Graphs []TeXGraph `yaml:"graphs"`
}
