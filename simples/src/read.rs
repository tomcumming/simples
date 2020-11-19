use disklog::LogPosition;

use crate::query::ParsedQuery;

pub struct ReadOptions {
    from: Option<LogPosition>,
    end_before: Option<LogPosition>,
    end_after: Option<LogPosition>,
    max_items: Option<usize>,
}

impl ReadOptions {
    pub fn from_query<'a>(mut query: ParsedQuery<'a>) -> Option<ReadOptions> {
        let mut options = ReadOptions {
            from: None,
            end_before: None,
            end_after: None,
            max_items: None,
        };
        for (k, v) in query.drain() {
            match k {
                "from" => options.from = Some(v.parse().ok()?),
                "end_before" => options.end_before = Some(v.parse().ok()?),
                "end_after" => options.end_after = Some(v.parse().ok()?),
                "max_items" => options.max_items = Some(v.parse().ok()?),
                _ => None?,
            }
        }
        Some(options)
    }
}

#[cfg(test)]
mod tests {
    use crate::query::parse_query_string;

    use super::*;

    #[test]
    fn parse_all_options() {
        let qs = "from=1&end_before=2&end_after=3&max_items=4";
        let options = parse_query_string(qs)
            .and_then(ReadOptions::from_query)
            .expect("Valid parse");
        assert_eq!(Some(1), options.from);
        assert_eq!(Some(2), options.end_before);
        assert_eq!(Some(3), options.end_after);
        assert_eq!(Some(4), options.max_items);
    }
}
