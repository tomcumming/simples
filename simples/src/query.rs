use std::collections::HashMap;

pub type ParsedQuery<'a> = HashMap<&'a str, &'a str>;

pub fn parse_query_string<'a>(query_string: &'a str) -> Option<ParsedQuery<'a>> {
    if query_string == "" {
        return Some(HashMap::new());
    }

    let mut result: ParsedQuery<'a> = HashMap::new();

    for pair in query_string.split("&") {
        if let [k, v] = *pair.split("=").collect::<Box<[_]>>() {
            if result.contains_key(k) {
                return None;
            } else {
                result.insert(k, v);
            }
        } else {
            return None;
        }
    }

    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_query_string() {
        assert_eq!(Some(HashMap::new()), parse_query_string(""));
    }
}
