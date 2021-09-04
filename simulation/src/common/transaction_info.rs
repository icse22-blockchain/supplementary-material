use regex::Regex;
use std::collections::HashSet;

lazy_static! {
    static ref RE: Regex =
        Regex::new(r"^.*?-(?P<hash>0x[a-fA-F0-9]+)$",).expect("Regex RE is correct");
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Target {
    Balance(String),
    Storage(String, String),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum AccessMode {
    Read,
    Write,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Access {
    pub target: Target,
    pub mode: AccessMode,
}

impl Access {
    fn from_string(raw: &str) -> Self {
        let mode = match &raw[0..1] {
            "R" => AccessMode::Read,
            "W" => AccessMode::Write,
            x => panic!("Unknown Access type: {}", x),
        };

        let target = match &raw[1..2] {
            "B" => {
                let address = raw[3..45].to_owned();
                Target::Balance(address)
            }
            "S" => {
                let address = raw[3..45].to_owned();
                let entry = raw[47..113].to_owned();
                Target::Storage(address, entry)
            }
            x => panic!("Unknown Target type: {}", x),
        };

        Access { target, mode }
    }

    pub fn storage_write(addr: &String, entry: &String) -> Access {
        Access {
            target: Target::Storage(addr.clone(), entry.clone()),
            mode: AccessMode::Write,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub tx_hash: String,
    pub accesses: HashSet<Access>,
}

pub fn parse_tx_hash(raw: &str) -> &str {
    RE.captures(raw)
        .expect(&format!("Expected to find tx hash in {}", raw))
        .name("hash")
        .map_or("", |m| m.as_str())
}

pub fn parse_accesses(raw: &str) -> HashSet<Access> {
    raw.trim_matches(|ch| ch == '{' || ch == '}')
        .split(", ")
        .map(Access::from_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use AccessMode::*;
    use Target::*;

    #[test]
    fn test_parse_access() {
        // read balance
        let raw = "RB(0x06012c8cf97bead5deae237070f9587f8e7a266d)";
        let parsed = Access::from_string(raw);

        assert_eq!(
            parsed,
            Access {
                mode: Read,
                target: Balance("0x06012c8cf97bead5deae237070f9587f8e7a266d".to_string())
            }
        );

        // write balance
        let raw = "WB(0x06012c8cf97bead5deae237070f9587f8e7a266d)";
        let parsed = Access::from_string(raw);

        assert_eq!(
            parsed,
            Access {
                mode: Write,
                target: Balance("0x06012c8cf97bead5deae237070f9587f8e7a266d".to_string())
            }
        );

        // read storage
        let raw = "RS(0x06012c8cf97bead5deae237070f9587f8e7a266d; 0xc56c286245a85e4048e082d091c57ede29ec05df707b458fe836e199193ff182)";
        let parsed = Access::from_string(raw);

        assert_eq!(
            parsed,
            Access {
                mode: Read,
                target: Storage(
                    "0x06012c8cf97bead5deae237070f9587f8e7a266d".to_string(),
                    "0xc56c286245a85e4048e082d091c57ede29ec05df707b458fe836e199193ff182"
                        .to_string()
                )
            }
        );

        // write storage
        let raw = "WS(0x06012c8cf97bead5deae237070f9587f8e7a266d; 0xc56c286245a85e4048e082d091c57ede29ec05df707b458fe836e199193ff182)";
        let parsed = Access::from_string(raw);

        assert_eq!(
            parsed,
            Access {
                mode: Write,
                target: Storage(
                    "0x06012c8cf97bead5deae237070f9587f8e7a266d".to_string(),
                    "0xc56c286245a85e4048e082d091c57ede29ec05df707b458fe836e199193ff182"
                        .to_string()
                )
            }
        );
    }

    #[test]
    fn test_parse_correct_tx_hash() {
        let raw = "6008052-0x5402ded8c99e72ed401e0a149e4240766bd08f4f9be689460d4dd7b163990486";
        let parsed = parse_tx_hash(raw);
        assert_eq!(
            parsed,
            "0x5402ded8c99e72ed401e0a149e4240766bd08f4f9be689460d4dd7b163990486"
        );
    }

    #[test]
    #[should_panic]
    fn test_parse_incorrect_tx_hash() {
        let raw = "6008052-6008052";
        let _parsed = parse_tx_hash(raw);
    }

    #[test]
    fn test_parse_accesses() {
        let raw = "{RB(0x06012c8cf97bead5deae237070f9587f8e7a266d), WS(0x06012c8cf97bead5deae237070f9587f8e7a266d; 0xc56c286245a85e4048e082d091c57ede29ec05df707b458fe836e199193ff182)}";
        let parsed = parse_accesses(raw);

        assert_eq!(parsed.len(), 2);

        assert!(parsed.contains(&Access {
            mode: Read,
            target: Balance("0x06012c8cf97bead5deae237070f9587f8e7a266d".to_string())
        }));

        assert!(parsed.contains(&Access {
            mode: Write,
            target: Storage(
                "0x06012c8cf97bead5deae237070f9587f8e7a266d".to_string(),
                "0xc56c286245a85e4048e082d091c57ede29ec05df707b458fe836e199193ff182".to_string()
            )
        }));
    }
}
