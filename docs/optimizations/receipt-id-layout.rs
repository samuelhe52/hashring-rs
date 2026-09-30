use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[allow(dead_code)]
struct RecordVersion {
    topology_epoch: u64,
    owner_sequence: u64,
    owner_node_id: String,
}
#[allow(dead_code)]
struct DedupEntry {
    key: Arc<[u8]>,
    fingerprint: [u8; 32],
    version: RecordVersion,
    deleted: bool,
    retained_bytes: usize,
    expires_at: Instant,
    required_acks: Vec<(u64, String, u64)>,
}

fn entry(i: usize, expires_at: Instant) -> DedupEntry {
    DedupEntry {
        key: Arc::from(i.to_be_bytes()),
        fingerprint: [0; 32],
        version: RecordVersion {
            topology_epoch: 1,
            owner_sequence: i as u64,
            owner_node_id: "node-1".into(),
        },
        deleted: false,
        retained_bytes: 0,
        expires_at,
        required_acks: Vec::new(),
    }
}

fn main() {
    let variant = std::env::args().nth(1).expect("string or arc");
    let n: usize = std::env::args()
        .nth(2)
        .expect("entry count")
        .parse()
        .unwrap();
    let expires_at = Instant::now() + Duration::from_secs(60);
    if variant == "arc" {
        let mut map: HashMap<Arc<str>, DedupEntry> = HashMap::with_capacity(n);
        let mut heap: BinaryHeap<Reverse<(Instant, Arc<str>)>> = BinaryHeap::with_capacity(n);
        for i in 0..n {
            let id: Arc<str> = format!("{:08x}-0000-4000-8000-{:012x}", i, i).into();
            heap.push(Reverse((expires_at, id.clone())));
            map.insert(id, entry(i, expires_at));
        }
        eprintln!("ready arc {} {}", map.len(), heap.len());
        std::io::stdin().read_line(&mut String::new()).unwrap();
        std::hint::black_box((map, heap));
    } else {
        let mut map: HashMap<String, DedupEntry> = HashMap::with_capacity(n);
        let mut heap: BinaryHeap<Reverse<(Instant, String)>> = BinaryHeap::with_capacity(n);
        for i in 0..n {
            let id = format!("{:08x}-0000-4000-8000-{:012x}", i, i);
            heap.push(Reverse((expires_at, id.clone())));
            map.insert(id, entry(i, expires_at));
        }
        eprintln!("ready string {} {}", map.len(), heap.len());
        std::io::stdin().read_line(&mut String::new()).unwrap();
        std::hint::black_box((map, heap));
    }
}
