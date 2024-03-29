use ego_tree::NodeRef;
use html5ever::{local_name, namespace_url, ns, QualName};
use std::collections::HashSet;
use url::Url;

mod element_node;
mod html_node;
use crate::parser::ParseResult;
use element_node::Node;
use html_node::Html;

pub const DEFAULT_DESC_LENGTH: usize = 256;

fn normalize_href(url: &str, href: &str) -> Option<String> {
    // Force HTTPS, crawler will fallback to HTTP if necessary.
    if let Ok(url) = Url::parse(url) {
        if href.starts_with("//") {
            // schema relative url
            if let Ok(url) = Url::parse(&format!("{}:{}", "https", href)) {
                return Some(url.to_string());
            }
        } else if href.starts_with("http://") || href.starts_with("https://") {
            // Force HTTPS, crawler will fallback to HTTP if necessary.
            if let Ok(url) = Url::parse(href) {
                let mut url = url;
                if url.scheme() == "http" {
                    url.set_scheme("https").expect("Unable to set HTTPS scheme");
                }
                return Some(url.to_string());
            }
        } else {
            // origin or directory relative url
            if let Ok(url) = url.join(href) {
                return Some(url.to_string());
            }
        }
    }

    log::debug!("Unable to normalize href: {} - {}", url.to_string(), href);
    None
}

/// Walk the DOM and grab all the p nodes
fn filter_p_nodes(root: &NodeRef<Node>, p_list: &mut Vec<String>) {
    for child in root.children() {
        let node = child.value();
        if node.is_element() {
            let element = node.as_element().unwrap();
            if element.name().eq_ignore_ascii_case("p") {
                let mut p_content = String::from("");
                let mut links = HashSet::new();
                filter_text_nodes(&child, &mut p_content, &mut links);

                if !p_content.is_empty() {
                    p_list.push(p_content);
                }
            }
        }

        if child.has_children() {
            filter_p_nodes(&child, p_list);
        }
    }
}

/// Filters a DOM tree into a text document used for indexing
fn filter_text_nodes(root: &NodeRef<Node>, doc: &mut String, links: &mut HashSet<String>) {
    // TODO: move to config file? turn into a whitelist?
    // TODO: Ignore list could also be updated per domain as well if needed
    let ignore_list: HashSet<String> = HashSet::from([
        "head".into(),
        "sup".into(),
        // Ignore elements that often don't contain relevant info
        "header".into(),
        "footer".into(),
        "nav".into(),
        // form elements
        "label".into(),
        "textarea".into(),
        "input".into(),
        // Ignore javascript/style nodes
        "script".into(),
        "noscript".into(),
        "style".into(),
    ]);

    let href_key = QualName::new(None, ns!(), local_name!("href"));
    let role_key = QualName::new(None, ns!(), local_name!("role"));
    let rel_key = QualName::new(None, ns!(), local_name!("rel"));

    let mut noindex_skip = false;

    for child in root.children() {
        if noindex_skip {
            continue;
        }

        let node = child.value();
        // Handle comments indicating we should skip parsing content nodes.
        // Rare, but happens in wikipedia exports.
        if node.is_comment() {
            if let Some(comment) = node.as_comment() {
                if comment.contains("htdig_noindex") {
                    noindex_skip = true;
                } else if comment.contains("/htdig_noindex") {
                    noindex_skip = false;
                }
            }
        } else if node.is_text() {
            doc.push_str(node.as_text().unwrap());
        } else if node.is_element() {
            // Ignore elements on the ignore list
            let element = node.as_element().unwrap();
            if ignore_list.contains(&element.name()) {
                continue;
            }

            // Ignore elements whose role is "navigation"
            // TODO: Filter out full-list of ARIA roles that are not content
            if element.attrs.contains_key(&role_key)
                && (element.attrs.get(&role_key).unwrap().to_string() == *"navigation"
                    || element.attrs.get(&role_key).unwrap().to_string() == *"contentinfo"
                    || element.attrs.get(&role_key).unwrap().to_string() == *"button")
            {
                continue;
            }

            // Save links
            if element.name() == "a" && element.attrs.contains_key(&href_key) {
                let href = element.attrs.get(&href_key).unwrap().to_string();
                let rel = if let Some(rel) = element.attrs.get(&rel_key) {
                    rel.to_string().to_lowercase()
                } else {
                    "follow".to_string()
                };

                // Ignore anchor links
                if !href.starts_with('#')
                    // ignore rels that tell us this link is not relevant
                    && rel != "nofollow" && rel != "external"
                {
                    links.insert(href.to_string());
                }
            } else if element.name() == "br" && !doc.ends_with(' ') {
                doc.push(' ');
            }

            if child.has_children() {
                filter_text_nodes(&child, doc, links);
                // Add spacing after elements.
                if !doc.ends_with(' ') {
                    doc.push(' ');
                }
            }
        }
    }
}

/// Processes the html document and pulls out the canonical url
pub fn process_canonical_url(url: &str, doc: &str) -> String {
    let parsed = Html::parse(doc);
    let link_tags = parsed.link_tags();

    match link_tags.get("canonical").map(|x| Url::parse(x)) {
        // Canonical URLs *must* be a full, valid URL
        Some(Ok(mut parsed)) => {
            // Ignore fragments
            parsed.set_fragment(None);
            parsed.to_string()
        }
        // Use the original URL if we are unable to determine the canonical URL from meta tags.
        _ => url.to_string(),
    }
}

/// Filters a DOM tree into a text document used for indexing
pub fn html_to_text(url: &str, doc: &str) -> ParseResult {
    let parsed = Html::parse(doc);
    let root = parsed.tree.root();
    // Meta tags
    let meta = parsed.meta();
    let link_tags = parsed.link_tags();
    // Content
    let title = parsed.title();
    let mut content = String::from("");
    let mut links = HashSet::new();
    filter_text_nodes(&root, &mut content, &mut links);
    // Trim extra spaces from content
    content = content.trim().to_string();
    // Normalize links
    links = links
        .into_iter()
        .flat_map(|href| normalize_href(url, &href))
        .collect();

    let mut description = if meta.contains_key("description") {
        meta.get("description").unwrap().to_string()
    } else if meta.contains_key("og:description") {
        meta.get("og:description").unwrap().to_string()
    } else {
        "".to_string()
    };

    if description.is_empty() && !content.is_empty() {
        // Extract first paragraph from content w/ text to use as the description
        let mut p_list = Vec::new();
        filter_p_nodes(&root, &mut p_list);

        let text = p_list.iter().find(|p_content| !p_content.trim().is_empty());
        if text.is_some() && !text.unwrap().is_empty() {
            description = text.unwrap_or(&String::from("")).trim().to_owned()
        } else if !content.is_empty() {
            // Still nothing? Grab the first 256 words-ish
            description = content
                .split(' ')
                .take(DEFAULT_DESC_LENGTH)
                .collect::<Vec<&str>>()
                .join(" ")
        }
    }

    // If there's a canonical URL on this page, attempt to determine whether it's valid.
    // More info about canonical URLS:
    // https://developers.google.com/search/docs/advanced/crawling/consolidate-duplicate-urls
    let canonical_url = match link_tags.get("canonical").map(|x| Url::parse(x)) {
        // Canonical URLs *must* be a full, valid URL
        Some(Ok(mut parsed)) => {
            // Ignore fragments
            parsed.set_fragment(None);
            Some(parsed.to_string())
        }
        // Use the original URL if we are unable to determine the canonical URL from meta tags.
        _ => Some(url.to_string()),
    };

    ParseResult::builder()
        .canonical_url(canonical_url)
        .content(content)
        .description(description)
        .links(links)
        .meta(meta)
        .title(title)
        .build()
}

#[cfg(test)]
mod test {
    use super::{html_to_text, normalize_href};
    use std::time::SystemTime;

    #[test]
    fn test_normalize_href() {
        let url = "https://example.com";

        assert_eq!(
            normalize_href(url, "http://foo.com"),
            Some("https://foo.com/".into())
        );
        assert_eq!(
            normalize_href(url, "https://foo.com"),
            Some("https://foo.com/".into())
        );
        assert_eq!(
            normalize_href(url, "//foo.com"),
            Some("https://foo.com/".into())
        );
        assert_eq!(
            normalize_href(url, "/foo.html"),
            Some("https://example.com/foo.html".into())
        );
        assert_eq!(
            normalize_href(url, "/foo"),
            Some("https://example.com/foo".into())
        );
        assert_eq!(
            normalize_href(url, "foo.html"),
            Some("https://example.com/foo.html".into())
        );
    }

    #[test]
    fn test_html_to_text() {
        let html = include_str!("../../../../fixtures/html/raw.html");
        let doc = html_to_text("https://oldschool.runescape.wiki", html);
        assert_eq!(doc.title, Some("Old School RuneScape Wiki".to_string()));
        assert_eq!(doc.meta.len(), 9);
        assert!(!doc.content.is_empty());
        assert_eq!(doc.links.len(), 58);
        println!("{:?}", doc.links);
    }

    #[test]
    fn test_html_to_text_large() {
        let start = SystemTime::now();
        let html = include_str!("../../../../fixtures/html/wikipedia_entry.html");
        let doc = html_to_text("https://example.com", html);

        let wall_time = start.elapsed().expect("elapsed");
        println!("wall_time: {}ms", wall_time.as_millis());

        assert_eq!(
            doc.title,
            Some("Rust (programming language) - Wikipedia".to_string())
        );
    }

    #[test]
    fn test_description_extraction() {
        let html = include_str!("../../../../fixtures/html/wikipedia_entry.html");
        let doc = html_to_text("https://example.com", html);

        assert_eq!(
            doc.title.unwrap(),
            "Rust (programming language) - Wikipedia"
        );
        assert_eq!(doc.description, "Rust  is a multi-paradigm , general-purpose programming language  designed for performance  and safety, especially safe concurrency . Rust is syntactically  similar to C++ , but can guarantee memory safety  by using a borrow checker  to validate references . Rust achieves memory safety without garbage collection , and reference counting  is optional. Rust has been called a systems programming  language, and in addition to high-level features such as functional programming  it also offers mechanisms for low-level  memory management .");

        let html = include_str!("../../../../fixtures/html/personal_blog.html");
        let doc = html_to_text("https://example.com", html);
        // ugh need to fix this
        assert_eq!(doc.description, "2020 July 15 - San Francisco |  855 words");
    }

    #[test]
    fn test_description_extraction_yc() {
        let html = include_str!("../../../../fixtures/html/summary_test.html");
        let doc = html_to_text("https://example.com", html);

        assert_eq!(doc.title.unwrap(), "Why YC");
        assert_eq!(doc.description, "March 2006, rev August 2009 Yesterday one of the founders we funded asked me why we started Y Combinator .  Or more precisely, he asked if we'd started YC mainly for fun. Kind of, but not quite.  It is enormously fun to be able to work with Rtm and Trevor again.  I missed that after we sold Viaweb, and for all the years after I always had a background process running, looking for something we could do together.  There is definitely an aspect of a band reunion to Y Combinator.  Every couple days I slip and call it \"Viaweb.\" Viaweb we started very explicitly to make money.  I was sick of living from one freelance project to the next, and decided to just work as hard as I could till I'd made enough to solve the problem once and for all.  Viaweb was sometimes fun, but it wasn't designed for fun, and mostly it wasn't.  I'd be surprised if any startup is. All startups are mostly schleps. The real reason we started Y Combinator is neither selfish nor virtuous.  We didn't start it mainly to make money; we have no idea what our average returns might be, and won't know for years.  Nor did we start YC mainly to help out young would-be founders, though we do like the idea, and comfort ourselves occasionally with the thought that if all our investments tank, we will thus have been doing something unselfish.  (It's oddly nondeterministic.) The");
    }
}
