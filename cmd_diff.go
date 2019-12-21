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
	var i string
	var x string
	var cmd = &cobra.Command{
		Use:   "diff [parquet file]",
		Short: "perform a diff on two parquet files",
		Long: `perform a diff on two parquet files, for example 
      > paraqeet diff foo.parquet -g gold.parquet -k MessageId`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			kc := split(k)
			f1, err := LoadFile(args[0], split(x), split(i), -1)
			if err != nil {
				log.Fatal(err)
			}
			f1.Sort(kc)
			f2, err := LoadFile(g, split(x), split(i), -1)
			if err != nil {
				log.Fatal(err)
			}
			f2.Sort(kc)
			out := os.Stdout
			if o != "" {
				of, err := os.Create(o)
				if err != nil {
					log.Fatal(err)
				}
				defer of.Close()
				out = of
			}
			d := NewDiffer(f1, f2, l, kc)
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
	cmd.Flags().StringVarP(&i, "include", "i", "", "the comma seperated column names to include, for example \"Foo,*Tiers\". (wildcard prefixes and suffixes are accepted)")
	cmd.Flags().StringVarP(&x, "excluse", "x", "", "the comma seperated column names to exclude, for example \"Foo,*Tiers\". (wildcard prefixes and suffixes are accepted)")
	cmd.Flags().IntVarP(&l, "limit", "l", 20, "limit the number of diffs that will be processed")
	return cmd
}
