package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var o string

func main() {
	var rootCmd = &cobra.Command{Use: "paraqeet"}
	rootCmd.PersistentFlags().StringVarP(&o, "output", "o", "", "output file for the results (defaults to standard out)")
	rootCmd.AddCommand(cmdDiff(), cmdInfo(), cmdCat())
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
