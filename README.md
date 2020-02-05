# paraqueet

paraqueet is a utility program to work with parquet files.

*Note that the diff command doesn't work on very large files due to memory constraints.*

## diff

Perform a diff between 2 parquet files

Specify a parquet file, a "gold" file to compare to, and a key column

```bash
paraqeet diff sample.parquet -g expected.parquet -k id
```

For more options: `paraqeet diff --help`

## cat

Print out the data in the parquet file

```bash
paraqeet cat sample.parquet
```

For more options, including alternate output formats: `paraqeet cat --help`

## info

display information about a parquet file

```bash
paraqeet info sample.parquet
```

For more options: `paraqeet info --help`

