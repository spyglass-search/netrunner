## netrunner

netrunner is a tool to help build, validate, & create archives for Spyglass lenses.


### Supported commands
To run the CLI application, use the following structure:
```
netrunner --lens-file <path to lens file> CMD
```

#### `check-urls`
This command will grab all the urls represented by this lens. This will be gathered
in either of two ways

1. Sitemap(s) for the domain(s) represented. If available, the tool will prioritize
using sitemaps.

2. Using data from the Internet Archive to determine what URLs are represented by
the rules in the lens.

The list will then be sorted alphabetically and outputted to stdout.

#### `crawl`
This will use the rules defined in the lens to crawl & archive the pages within.

This is primarily used as way to create cached web archives that can be distributed w/
community created lenses so that others don't have to crawl the same pages.

#### `validate`
This will validate the rules inside a lens file and, if previouisly crawled, the
cached web archive for this lens.