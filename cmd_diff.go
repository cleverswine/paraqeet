package main

import (
	"fmt"
	"log"
	"os"

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
		Long: `perform a diff on two parquet files, for example 
  > paraqeet diff foo.parquet -g gold.parquet -k MessageId`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			kc := split(k)
			sc := split(s)
			if sc == nil {
				sc = kc
			}
			ic := split(i)
			f1, err := NewFile(args[0], -1, sc, ic)
			if err != nil {
				log.Fatal(err)
			}
			f2, err := NewFile(g, -1, sc, ic)
			if err != nil {
				log.Fatal(err)
			}
			out := os.Stdout
			if o != "" {
				of, err := os.Create(o)
				if err != nil {
					log.Fatal(err)
				}
				defer of.Close()
				out = of
			}
			d := NewDiffer(f1, f2, l, kc, ic)
			result := d.Diff()
			for _, res := range result {
				res.String(out)
			}
			fmt.Fprintf(out, "\nThere were a total of %d rows with differences.\n", len(result))
		},
	}
	cmd.Flags().StringVarP(&g, "gold", "g", "", "the \"gold\" parquet file to compare with")
	cmd.MarkFlagRequired("gold")
	cmd.Flags().StringVarP(&k, "keys", "k", "", "the comma seperated key column names for joining the files, for example \"MessageId,SenderAccountId\"")
	cmd.MarkFlagRequired("keys")
	cmd.Flags().StringVarP(&s, "sort", "s", "", "the comma seperated sort-by column names (if different than the key columns)")
	cmd.Flags().StringVarP(&i, "ignore", "i", "", "the comma seperated column names to ignore, for example \"Foo,*Tiers\". (wildcard prefixes and suffixes are accepted)")
	cmd.Flags().IntVarP(&l, "limit", "l", 20, "limit the number of diffs that will be processed")
	return cmd
}
