package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/spf13/cobra"
)

func cmdInfo() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "info [parquet file]",
		Short: "display information about a parquet file",
		Long:  "display information about a parquet file",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			out := os.Stdout
			if o != "" {
				of, err := os.Create(o)
				if err != nil {
					log.Fatal(err)
				}
				defer of.Close()
				out = of
			}
			f1, err := NewFile(args[0], -1, nil, nil)
			if err != nil {
				log.Fatal(err)
			}
			jd := json.NewEncoder(out)
			err = jd.Encode(f1.Info())
			if err != nil {
				log.Fatal(err)
			}

		},
	}
	return cmd
}
