package main

import (
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func cmdDiff() *cobra.Command {
	var g string
	var l int
	var k string
	var s string
	var i string
	var cmd = &cobra.Command{
		Use:   "diff [parquet file]",
		Short: "perform a diff on two parquet files",
		Long:  "perform a diff on two parquet files",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f1, err := NewParaqeet(args[0])
			if err != nil {
				log.Fatal(err)
			}
			defer f1.Close()
			f2, err := NewParaqeet(g)
			if err != nil {
				log.Fatal(err)
			}
			defer f2.Close()
			out := os.Stdout
			if o != "" {
				of, err := os.Create(o)
				if err != nil {
					log.Fatal(err)
				}
				defer of.Close()
				out = of
			}
			kc := strings.Split(k, ",")
			sc := strings.Split(s, ",")
			ic := strings.Split(i, ",")
			d := NewDiffer(f1, f2, l, kc, sc, ic)
			result := d.Diff()
			for _, res := range result {
				res.String(out)
			}
		},
	}
	cmd.Flags().StringVarP(&g, "gold", "g", "", "the \"gold\" parquet file to compare with")
	cmd.MarkFlagRequired("gold")
	cmd.Flags().StringVarP(&k, "keys", "k", "", "the comma seperated key column names for joining the files")
	cmd.MarkFlagRequired("keys")
	cmd.Flags().StringVarP(&s, "sort", "s", "", "the comma seperated sort-by column names (if different than key columns)")
	cmd.Flags().StringVarP(&i, "ignore", "i", "", "the comma seperated column names to ignore")
	cmd.Flags().IntVarP(&l, "limit", "l", 20, "limit the number of diffs that will be processed")
	return cmd
}
