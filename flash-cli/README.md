# flash-cli

Command-line interface for the FLASH read merging algorithm implemented in Rust. This binary wraps
the `flash-lib` crate to merge paired FASTQ reads using the same defaults as the original FLASH
utility.

## Usage

```bash
flash-cli input1.fq input2.fq --output-dir out --output-prefix merged
```

For all available options, run:

```bash
flash-cli --help
```

## License

Released under the MIT license. See the repository root for details.
