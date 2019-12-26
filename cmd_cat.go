package main

import (
	"log"
	"os"

	"github.com/spf13/cobra"
)

func cmdCat() *cobra.Command {
	var format string
	var limit int
	var include string
	var exclude string
	var sortBy string
	var cmd = &cobra.Command{
		Use:   "cat [parquet file]",
		Short: "display rows of a parquet file",
		Long:  "display rows of a parquet file",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			out := os.Stdout
			if outFile != "" {
				of, err := os.Create(outFile)
				if err != nil {
					log.Fatal(err)
				}
				defer of.Close()
				out = of
			}
			file, err := LoadFile(args[0], split(exclude), split(include), limit)
			if err != nil {
				log.Fatal(err)
			}
			if format == "json" {
				file.ToJSON(out)
			} else {
				file.ToTable(out)
			}
		},
	}
	cmd.Flags().StringVarP(&format, "format", "f", "table", "the format to display [table|json]")
	cmd.Flags().IntVarP(&limit, "limit", "l", 10, "limit the number of rows to show")
	cmd.Flags().StringVarP(&sortBy, "sort", "s", "", "the comma seperated sort-by column names")
	cmd.Flags().StringVarP(&include, "include", "i", "", "the comma seperated column names to include, for example \"Foo,*Tiers\". (wildcard prefixes and suffixes are accepted)")
	cmd.Flags().StringVarP(&exclude, "exclude", "x", "", "the comma seperated column names to exclude, for example \"Foo,*Tiers\". (wildcard prefixes and suffixes are accepted)")
	return cmd
}
