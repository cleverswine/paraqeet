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
			f1, err := LoadFile(args[0], nil, nil, 1)
			if err != nil {
				log.Fatal(err)
			}
			err = json.NewEncoder(out).Encode(f1)
			if err != nil {
				log.Fatal(err)
			}
		},
	}
	return cmd
}
