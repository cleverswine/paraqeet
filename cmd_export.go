package main

import (
	"log"
	"os"

	"github.com/spf13/cobra"
)

func cmdExport() *cobra.Command {
	var f string
	var o string
	var l int
	var cmd = &cobra.Command{
		Use:   "export",
		Short: "export a parquet file to json",
		Long:  "export a parquet file to json",
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
			f1, err := NewParaqeet(f)
			if err != nil {
				log.Fatal(err)
			}
			defer f1.Close()
			f1.ToJson(l, out)
		},
	}
	cmd.Flags().StringVarP(&f, "file", "f", "", "the parquet file to export")
	cmd.MarkFlagRequired("file")
	cmd.Flags().StringVarP(&o, "output", "o", "", "output file for the result (defaults to standard out)")
	cmd.Flags().IntVarP(&l, "limit", "l", -1, "limit the number of rows to export")
	return cmd
}
