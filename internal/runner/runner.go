package runner

import (
	"fmt"

	"github.com/Milover/foam-postprocess/internal/input"
	"github.com/Milover/foam-postprocess/internal/output"
	"github.com/Milover/foam-postprocess/internal/process"
)

func Run(config *Config) error {
	config.Input.Log = config.Log
	for i := range config.Process {
		config.Process[i].Log = config.Log
	}
	df, err := input.CreateDataFrame(&config.Input)
	if err != nil {
		return fmt.Errorf("error creating data frame: %w", err)
	}
	if err = process.Process(df, config.Process); err != nil {
		return fmt.Errorf("error processing data frame: %w", err)
	}
	if err = output.Output(df, &config.Output); err != nil {
		return fmt.Errorf("output error: %w", err)
	}
	return nil
}
