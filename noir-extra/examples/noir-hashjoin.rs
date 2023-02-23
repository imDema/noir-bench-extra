use rand::{rngs::SmallRng, Rng, SeedableRng};

use noir::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    env_logger::init();
    let (config, args) = EnvironmentConfig::from_args();

    // command-line args: numbers of nodes and edges in the random graph.
    let keys: u64 = args[0].parse().unwrap();
    let vals: u64 = args[1].parse().unwrap();
    let batch: u64 = args[2].parse().unwrap();

    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();

    let s1 = env.stream(ParallelIteratorSource::new(move |index, peers| {
        let mut r1: SmallRng = SeedableRng::seed_from_u64(index as u64);
        (0..(vals / peers)).map(move |_| (r1.gen_range(0..keys), r1.gen_range(0..keys)))
    }));

    let s2 = env.stream(ParallelIteratorSource::new(move |index, peers| {
        let mut r2: SmallRng = SeedableRng::seed_from_u64(index as u64 + 0xdeadbeef);
        (0..(vals / peers)).map(move |_| (r2.gen_range(0..keys), r2.gen_range(0..keys)))
    }));

    s1.batch_mode(BatchMode::fixed(batch as usize))
        .join(s2, |x: &(u64, u64)| x.0, |x: &(u64, u64)| x.0)
        .drop_key()
        .map(|q| (q.0 .1, q.1 .1))
        .for_each(|x| println!("{x:?}"));

    env.execute();

    // println!("{:?}", result.get().unwrap())
}
