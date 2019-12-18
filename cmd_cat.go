package main

import (
	"log"
	"os"

	"github.com/spf13/cobra"
)

func cmdCat() *cobra.Command {
	var f string
	var l int
	var cmd = &cobra.Command{
		Use:   "cat [parquet file]",
		Short: "display rows of a parquet file",
		Long:  "display rows of a parquet file",
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
			f1, err := NewFile(args[0], l, nil)
			if err != nil {
				log.Fatal(err)
			}
			if f == "json" {
				f1.ToJSON(out)
			} else {
				f1.ToTable(out)
			}
		},
	}
	cmd.Flags().StringVarP(&f, "format", "f", "table", "the format to display [table|json]")
	cmd.Flags().IntVarP(&l, "limit", "l", 10, "limit the number of rows to show")
	return cmd
}
