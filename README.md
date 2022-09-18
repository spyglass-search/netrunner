## netrunner

`netrunner` is a tool to help build, validate, & create archives for Spyglass lenses.

## Installing the CLI

```
cargo install --path .
```

## Running the CLI

```
netrunner 0.1.0

USAGE:
    netrunner --lens-file <FILE> <SUBCOMMAND>

OPTIONS:
    -h, --help                Print help information
    -l, --lens-file <FILE>    Lens file
    -V, --version             Print version information

SUBCOMMANDS:
    check-urls    Grabs all the URLs represented by <lens-file> for review
    crawl         Crawls & creates a web archive for the pages represented by <lens-file>
    help          Print this message or the help of the given subcommand(s)
    validate      Validate the lens file and, if available, the cached web archive for
                      <lens-file>
```


## Commands in depth

### `check-urls`
This command will grab all the urls represented by this lens. This will be gathered
in either of two ways:

1. Sitemap(s) for the domain(s) represented. If available, the tool will prioritize
using sitemaps or

2. Using data from the Internet Archive to determine what URLs are represented by
the rules in the lens.

The list will then be sorted alphabetically and outputted to stdout. This is a great
way to check whether the URLs that will be crawled & indexed are what you're expecting
for a lens.


### `crawl`
This will use the rules defined in the lens to crawl & archive the pages within.

This is primarily used as way to create cached web archives that can be distributed w/
community created lenses so that others don't have to crawl the same pages.


### `validate`
This will validate the rules inside a lens file and, if previouisly crawled, the
cached web archive for this lens.