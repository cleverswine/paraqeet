package main

import (
	"log"
	"os"

	"github.com/spf13/cobra"
)

func cmdInfo() *cobra.Command {
	var f string
	var o string
	var cmd = &cobra.Command{
		Use:   "info",
		Short: "display information about a parquet file",
		Long:  "display information about a parquet file",
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
			ShowSchema(f, out)
		},
	}
	cmd.Flags().StringVarP(&f, "file", "f", "", "the parquet file to operate on")
	cmd.MarkFlagRequired("file")
	cmd.Flags().StringVarP(&o, "output", "o", "", "output file for the results (defaults to standard out)")
	return cmd
}
