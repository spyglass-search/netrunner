## netrunner

`netrunner` is a tool to help build, validate, & create archives for
[Spyglass][spyglass-link] lenses.

Lenses are a [simple set of rules][lens-docs] that tell a crawler which URLs it
should crawl. Combined w/ data from sitemaps and/or the Internet Archive, `netrunner`
can crawl and created an archive of the pages represented by the lens.

In Spyglass, this is used to create a personalized search engine that only crawls
& indexes sites/pages/data that you're interested in.

[spyglass-link]: https://github.com/a5huynh/spyglass
[lens-docs]: https://docs.spyglass.fyi/usage/lenses/index.html

## Installing the CLI

From [crates.io](https://crates.io/crates/spyglass-netrunner):
```
cargo install spyglass-netrunner
```

or from source:
```
cargo install --path .
```


## Running the CLI

```
netrunner 0.1.8
Andrew Huynh <andrew@spyglass.fyi>
A small CLI utility to help build lenses for spyglass

Usage: netrunner [OPTIONS] <COMMAND>

Commands:
  check-domain  Print out useful information about a domain, such as whether there is an rss feed, an robots.txt, and a sitemap
  check-urls    Grabs all the URLs represented by <lens-file> for review
  clean         Removes temporary directories/files
  crawl         Crawls & creates a web archive for the pages represented by <lens-file>
  validate      Validate the lens file and, if available, the cached web archive for <lens-file>
  help          Print this message or the help of the given subcommand(s)

Options:
  -l, --lens-file <FILE>            Lens file
  -s, --s3-bucket <S3_BUCKET_NAME>  Upload finished archive to S3
  -h, --help                        Print help information
  -V, --version                     Print version information
```


## Commands in depth

### `check-domain`
This will run through a couple checks for a particular domain, pulling out URL paths
(if available) to `robots.txt`, `sitemap.xml`, and any RSS feeds for the domain. This
data is used in the `check-urls` and `crawl` commands to create the WARC for a particular
lens.

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