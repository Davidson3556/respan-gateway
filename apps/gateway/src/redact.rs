//! What may be shown of a value that can hold a secret.

/// `url` without its userinfo, query and fragment: where a provider is,
/// not the credentials a URL can carry. Not a URL: shown as it is.
pub fn url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let end = rest.find(['?', '#']).unwrap_or(rest.len());
    let (location, _) = rest.split_at(end);
    let (authority, path) = location.split_at(location.find('/').unwrap_or(location.len()));
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    format!("{scheme}://{host}{path}")
}

#[cfg(test)]
mod tests {
    use super::url;

    #[test]
    fn userinfo_query_and_fragment_are_dropped() {
        assert_eq!(
            url("https://u:p@h.example:8/v1/x?key=s#f"),
            "https://h.example:8/v1/x"
        );
        assert_eq!(
            url("https://api.openai.com/v1"),
            "https://api.openai.com/v1"
        );
        assert_eq!(url("not a url"), "not a url");
    }
}
