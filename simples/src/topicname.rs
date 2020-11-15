pub struct TopicName(String);

const MAX_LENGTH: usize = 32;

fn valid_topic_name_char(ch: char) -> bool {
    ch.is_alphanumeric() || ['_', '%'].contains(&ch)
}

impl TopicName {
    pub fn parse(name: &str) -> Option<TopicName> {
        let short_enough = name.len() <= MAX_LENGTH;
        let valid_chars = name.chars().all(valid_topic_name_char);

        // Might need to check for windows restricted names?

        if short_enough && valid_chars {
            Some(TopicName(name.to_string()))
        } else {
            None
        }
    }

    pub fn to_str(&self) -> &str {
        self.0.as_ref()
    }
}
