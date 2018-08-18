use bit_set::BitSet;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fmt;
use std::iter::FromIterator;
use std::sync::RwLock;

pub type EventSet = BitSet;
type Pattern = Vec<EventSet>;
type List = Vec<Record>;

/// Record contains sid and eid of an EventSet
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Record {
    sid: u32,
    eid: i32,
}

impl Record {
    pub fn new(sid: u32, eid: i32) -> Self {
        Self { sid, eid }
    }
}

/// Sequential PAttern Discovery using Equivalence classes, a.k.a Spade, is an vertical data format-
/// based sequential pattern method for Data Mining.
///
/// Ref: http://www.philippe-fournier-viger.com/spmf/SPADE.pdf for more information.
///
/// Although a Spade::new() and Spade::insert() is provided, an FromIterator way is recommended.
/// You need to manually maintains the order of Records when using insert, while FromIterator will
/// do sort during construct.
///
/// # Example:
/// ```rust
///
/// const DATA: &[(u32, i32, u8)] = &[
///     (0, 1, 0b10000000),
///     (0, 2, 0b11000000),
///     (1, 1, 0b01000000),
///     (1, 1, 0b00100000),
///     (2, 1, 0b11100000),
/// ];
///
/// let spade: Spade = DATA
///     .iter()
///     .map(|&(sid, eid, event_set)| {
///         let record = Record::new(sid, eid);
///         let events = EventSet::from_bytes(&[event_set]);
///         (record, events)
///     })
///     .collect();
///
/// ```
pub struct Spade {
    stack: HashMap<Pattern, List>,
    store: HashMap<Pattern, List>,
}

impl Spade {
    pub fn new() -> Self {
        Self {
            stack: HashMap::new(),
            store: HashMap::new(),
        }
    }

    pub fn insert(&mut self, record: Record, event_set: BitSet) {
        for event_id in event_set.iter() {
            let mut event = BitSet::new();
            event.insert(event_id);
            self.stack.entry(vec![event]).or_default().push(record);
        }
    }

    fn candidate(&self, min_sup: usize) -> Vec<(&Pattern, &Pattern)> {
        let mut result = Vec::new();

        let patterns: Vec<&Pattern> = self
            .stack
            .iter()
            .filter_map(|(pattern, list)| {
                if list.len() > min_sup {
                    Some(pattern)
                } else {
                    None
                }
            })
            .collect();

        let mut iters = patterns.into_iter();
        while let Some(prefix_pattern) = iters.next() {
            result.push((prefix_pattern, prefix_pattern));

            let mut iters = iters.clone();
            while let Some(suffix_pattern) = iters.next() {
                result.push((prefix_pattern, suffix_pattern));
            }
        }

        result
    }

    /// Enumerate the next step of BFS, with min_sup pruning
    pub fn next(&mut self, min_sup: usize) {
        let result = RwLock::new(HashMap::new());

        self.candidate(min_sup)
            .into_par_iter()
            .for_each(|(pattern_a, pattern_b)| {
                let last_idx_a = pattern_a.len() - 1;
                let last_idx_b = pattern_b.len() - 1;

                let list_a = &self.stack[pattern_a];
                let list_b = &self.stack[pattern_b];

                let prefix_a = &pattern_a[..last_idx_a];
                let prefix_b = &pattern_b[..last_idx_b];

                let suffix_a = &pattern_a[last_idx_a];
                let suffix_b = &pattern_b[last_idx_b];

                // Assume P -> A & P -> B
                if last_idx_a == last_idx_b && prefix_a == prefix_b {
                    // Produce P -> A -> B
                    let mut pattern = pattern_a.clone();
                    pattern.push(suffix_b.clone());

                    // Check if already calc
                    if !result.read().unwrap().contains_key(&pattern) {
                        let list = join_extend(list_a, list_b);
                        result.write().unwrap().insert(pattern, list);
                    }

                    // Assume P -> A & P -> B, A != B
                    if pattern_a != pattern_b {
                        // Produce P -> B -> A
                        let mut pattern = pattern_b.clone();
                        pattern.push(suffix_a.clone());

                        // Check if already calc
                        if !result.read().unwrap().contains_key(&pattern) {
                            let list = join_extend(list_b, list_a);
                            result.write().unwrap().insert(pattern, list);
                        }

                        // Produce P -> AB
                        let mut pattern = prefix_a.to_vec();
                        let mut last = suffix_a.clone();
                        last.union_with(suffix_b);
                        pattern.push(last);

                        // Check if already calc
                        if !result.read().unwrap().contains_key(&pattern) {
                            let list = join_expand(list_a, list_b);
                            result.write().unwrap().insert(pattern, list);
                        }
                    }
                }

                // Maybe PA & P -> B
                if last_idx_a + 1 == last_idx_b && &prefix_b[..last_idx_b - 1] == prefix_a {
                    // Produce PA -> B
                    let mut pattern = pattern_a.clone();
                    pattern.push(suffix_b.clone());

                    // Check if already calc
                    if !result.read().unwrap().contains_key(&pattern) {
                        let list = join_extend(list_a, list_b);
                        result.write().unwrap().insert(pattern, list);
                    }
                }

                if last_idx_b + 1 == last_idx_a && &prefix_a[..last_idx_a - 1] == prefix_b {
                    // Produce PA -> B
                    let mut pattern = pattern_b.clone();
                    pattern.push(suffix_a.clone());

                    // Check if already calc
                    if !result.read().unwrap().contains_key(&pattern) {
                        let list = join_extend(list_b, list_a);
                        result.write().unwrap().insert(pattern, list);
                    }
                }
            });

        let stack = result.into_inner().unwrap();
        self.store.extend(stack.clone().into_iter());
        self.stack = stack;
    }

    /// Produce an impl Iterator<Item = (Pattern, usize)> of each Pattern and Support
    pub fn report(&self) -> impl Iterator<Item = (Pattern, usize)> {
        let mut result: Vec<_> = self
            .store
            .par_iter()
            .map(|(k, v)| {
                let pattern = k.clone();
                let support = v.len();
                let sort_key = k.iter().map(|x| x.iter().count()).product();
                (pattern, support, sort_key)
            })
            .collect();
        result.par_sort_unstable_by_key(|&(_, support, sort_key): &(_, usize, usize)| {
            (-(sort_key as isize), -(support as isize))
        });
        result
            .into_iter()
            .map(|(pattern, support, _)| (pattern, support))
    }
}

// Join P->B, PA, Produce PA->B
// Join P->A, P->B, Produce P->A->B
fn join_extend(a: &List, b: &List) -> List {
    let mut result: List = Vec::new();
    let mut i = 0;
    let mut j = 0;

    while j < b.len() && i < a.len() {
        if a[i].sid < b[j].sid {
            i += 1;
            continue;
        };

        if a[i].sid > b[j].sid {
            j += 1;
            continue;
        };

        if a[i].eid < b[j].eid {
            result.push(b[j]);
        };

        j += 1;
    }

    result
}

// Join PA & PB, Produce PAB
// Join P->A & P->B, Produce P->AB
fn join_expand(a: &List, b: &List) -> List {
    let mut result: List = Vec::new();
    let mut i = 0;
    let mut j = 0;

    while j < b.len() && i < a.len() {
        if a[i].sid < b[j].sid {
            i += 1;
            continue;
        };

        if a[i].sid > b[j].sid {
            j += 1;
            continue;
        };

        if a[i].eid < b[j].eid {
            i += 1;
            continue;
        };

        if a[i].eid > b[j].eid {
            j += 1;
            continue;
        };

        result.push(b[j]);
        i += 1;
        j += 1;
    }

    result
}

impl FromIterator<(Record, EventSet)> for Spade {
    fn from_iter<I: IntoIterator<Item = (Record, EventSet)>>(iter: I) -> Self {
        let mut stack: HashMap<Pattern, List> = HashMap::new();
        for (record, event_set) in iter {
            for event_id in event_set.iter() {
                let mut event = EventSet::new();
                event.insert(event_id);

                stack.entry(vec![event]).or_default().push(record);
            }
        }
        stack.values_mut().for_each(|v| {
            v.par_sort_unstable();
            v.dedup();
        });
        let store = stack.clone();
        Self { stack, store }
    }
}

impl fmt::Debug for Spade {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "\nSpade Stack\n\n")?;
        let mut patterns: Vec<&Pattern> = self.stack.keys().collect();
        patterns.par_sort_unstable();

        for pattern in patterns {
            let list = &self.stack[pattern];
            if list.len() < 30 {
                continue;
            }

            write!(f, "Pattern: ")?;
            write!(f, "{:?}", &pattern[0])?;
            for event_set in &pattern[1..] {
                write!(f, " -> {:?}", event_set)?;
            }
            write!(f, ", Support: {}\n", list.len())?;

            // for record in list {
            //     write!(f, "{}, {}\n", record.sid, record.eid)?;
            // }
            // write!(f, "\n")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{BitSet, Record, Spade};

    const DATA: &[(u32, i32, u8)] = &[
        (0, 1, 0b10000000),
        (0, 2, 0b11000000),
        (1, 1, 0b01000000),
        (1, 1, 0b00100000),
        (2, 1, 0b11100000),
    ];

    #[test]
    fn test_collect_into_spade() {
        let spade: Spade = DATA
            .iter()
            .map(|&(sid, eid, event_set)| {
                let record = Record::new(sid, eid);
                let events = BitSet::from_bytes(&[event_set]);
                (record, events)
            })
            .collect();

        let mut event_set = BitSet::new();
        event_set.insert(0);

        assert_eq!(
            spade.stack[&vec![event_set]],
            vec![Record::new(0, 1), Record::new(0, 2), Record::new(2, 1)]
        );
    }
}
