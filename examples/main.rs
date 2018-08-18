extern crate sequential_pattern;
use sequential_pattern::{EventSet, Record, Spade};

const DATA: &[(u32, i32, u8)] = &[
    (0, 1, 0b10000000),
    (0, 2, 0b11000000),
    (1, 1, 0b01000000),
    (1, 1, 0b00100000),
    (2, 1, 0b11100000),
    (2, 2, 0b01100000),
];

fn main() {
    let mut spade: Spade = DATA
        .iter()
        .map(|&(sid, eid, event_set)| {
            let record = Record::new(sid, eid);
            let event_set = EventSet::from_bytes(&[event_set]);
            (record, event_set)
        })
        .collect();

    spade.next(0);
    spade.next(0);
    spade.next(0);
    spade.next(0);
    println!("{:?}", spade);

    for (pattern, support) in spade.report() {
        println!("Pattern: {:?}, Support: {}", pattern, support);
    }
}
