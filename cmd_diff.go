package main

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func cmdDiff() *cobra.Command {
	var gold string
	var limit int
	var keys string
	var include string
	var exclude string
	var cmd = &cobra.Command{
		Use:   "diff [parquet file]",
		Short: "perform a diff on two parquet files",
		Long: `perform a diff on two parquet files, for example 
      > paraqeet diff foo.parquet -g gold.parquet -k MessageId`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			keyCols := split(keys)
			var wg errgroup.Group
			var f1, f2 *File
			wg.Go(func() error {
				var err error
				f1, err = LoadFile(args[0], split(exclude), split(include), -1)
				if err != nil {
					return err
				}
				f1.Sort(keyCols)
				return nil
			})
			wg.Go(func() error {
				var err error
				f2, err = LoadFile(gold, split(exclude), split(include), -1)
				if err != nil {
					return err
				}
				f2.Sort(keyCols)
				return nil
			})
			err := wg.Wait()
			if err != nil {
				log.Fatal(err)
			}
			out := os.Stdout
			if outFile != "" {
				of, err := os.Create(outFile)
				if err != nil {
					log.Fatal(err)
				}
				defer of.Close()
				out = of
			}
			d := NewDiffer(f1, f2, limit, keyCols)
			result := d.Diff()
			for _, res := range result {
				res.String(out)
			}
			fmt.Fprintf(out, "\nThere were a total of %d rows with differences.\n", len(result))
		},
	}
	cmd.Flags().StringVarP(&gold, "gold", "g", "", "the \"gold\" parquet file to compare with")
	cmd.MarkFlagRequired("gold")
	cmd.Flags().StringVarP(&keys, "keys", "k", "", "the comma seperated key column names for joining the files, for example \"MessageId,SenderAccountId\"")
	cmd.MarkFlagRequired("keys")
	cmd.Flags().StringVarP(&include, "include", "i", "", "the comma seperated column names to include, for example \"Foo,*Tiers\". (wildcard prefixes and suffixes are accepted)")
	cmd.Flags().StringVarP(&exclude, "exclude", "x", "", "the comma seperated column names to exclude, for example \"Foo,*Tiers\". (wildcard prefixes and suffixes are accepted)")
	cmd.Flags().IntVarP(&limit, "limit", "l", 20, "limit the number of diffs that will be processed")
	return cmd
}
