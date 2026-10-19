use std::hint::black_box;
use std::time::Instant;

fn main() {
    const N: u32 = 2_000_000;
    for round in 0..3 {
        let started = Instant::now();
        for _ in 0..N {
            black_box(uuid::Uuid::new_v4().to_string());
        }
        let uuid_ns = started.elapsed().as_nanos() as f64 / f64::from(N);
        let started = Instant::now();
        for _ in 0..N {
            black_box(format!("{:032x}", rand::random::<u128>()));
        }
        let rand_ns = started.elapsed().as_nanos() as f64 / f64::from(N);
        println!("round {round}: uuid v4 {uuid_ns:.1} ns/id, rand u128 hex {rand_ns:.1} ns/id");
    }
}
