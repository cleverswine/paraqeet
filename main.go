package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var o string

func main() {
	// pq, err := LoadFile("sample_files/columnMismatch1.parquet", nil, []string{"*Id"}, -1)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// pq.ToTable(os.Stdout)
	var rootCmd = &cobra.Command{Use: "paraqeet"}
	rootCmd.PersistentFlags().StringVarP(&o, "output", "o", "", "output file for the results (defaults to standard out)")
	rootCmd.AddCommand(cmdDiff(), cmdInfo(), cmdCat())
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
