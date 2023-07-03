package output

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/go-gota/gota/dataframe"
	"github.com/sirupsen/logrus"
)

func outDir(config *Config) (string, error) {
	if err := os.MkdirAll(filepath.Clean(config.Directory), 0755); err != nil {
		return "", err
	}
	return config.Directory, nil
}

// WriteCSV writes df to a CSV file, using options from the config.
// FIXME: LaTeX has an upper size limit for CSV files that it can handle
// so the output should be decimated down to this size if it's too large.
func WriteCSV(df *dataframe.DataFrame, config *Config) error {
	csv, err := outDir(config)
	if err != nil {
		return err
	}
	if len(config.TableFile) == 0 {
		return err
	}
	csv = filepath.Join(csv, config.TableFile+".csv")
	config.Log.WithFields(logrus.Fields{
		"file": csv,
	}).Debug("writing csv")
	w, err := os.Create(csv)
	if err != nil {
		return err
	}
	if err := df.WriteCSV(w, dataframe.WriteHeader(true)); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

// WriteGraphFiles writes graph files, using options from the config.
func WriteGraphFiles(config *Config) error {
	outdir, err := outDir(config)
	if err != nil {
		return err
	}
	// we can only write LaTeX graphs
	for i := range config.Graphs {
		g := &config.Graphs[i]
		if len(g.TableFile) == 0 {
			g.TableFile = filepath.Join(outdir, config.TableFile+".csv") // TODO
		}
		// write graph file
		f := filepath.Join(outdir, g.Name+".tex") // TODO
		config.Log.WithFields(logrus.Fields{
			"file": f,
		}).Debug("writing graph file")
		w, errCreate := os.Create(f)
		errGraph := WriteTeXGraph(w, g)
		errClose := w.Close()
		err = errors.Join(err, errCreate, errGraph, errClose)
	}
	return err
}

// GenerateGraphs generates the actual graphs, e.g., PDFs from TeX files.
func GenerateGraphs(config *Config) error {
	outdir, err := outDir(config)
	if err != nil {
		return err
	}
	// we can only compile LaTeX graphs
	for i := range config.Graphs {
		g := &config.Graphs[i]
		f := filepath.Join(outdir, g.Name+".tex")
		config.Log.WithFields(logrus.Fields{
			"file": f,
		}).Debug("generating graph")
		if _, e := os.Stat(f); e != nil {
			err = errors.Join(err, e)
			continue
		}
		if e := GenerateTeXGraph(f); e != nil {
			err = errors.Join(err, e)
		}
	}
	return err
}
