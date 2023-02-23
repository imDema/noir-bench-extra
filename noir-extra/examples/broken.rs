use std::{env::temp_dir, fs::File, time::Instant};

use rand::{rngs::SmallRng, Rng, SeedableRng};

use noir::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// cargo build --example broken && RUST_LOG="warn" hyperfine --min-runs 100 'target/debug/examples/broken -r ../noir-config-localhost.yml 64 200'
fn main() {
    let start = Instant::now();
    let (config, args) = EnvironmentConfig::from_args();

    let mut log_file = temp_dir();
    log_file.push("noir");
    log_file.push(format!("noir-{:02x}.log", config.host_id.unwrap_or(0xffff)));
    let log_file = File::options()
        .create(true)
        .write(true)
        .truncate(true)
        .open(log_file)
        .unwrap();
    let target = env_logger::Target::Pipe(Box::new(log_file));
    env_logger::builder().target(target).init();

    // command-line args: numbers of nodes and edges in the random graph.
    let nodes: u64 = args[0].parse().unwrap();
    let edges: u64 = args[1].parse().unwrap();

    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();

    let source = env
        .stream_par_iter(move |index, peers| {
            let mut rng1: SmallRng = SeedableRng::seed_from_u64(index as u64);
            log::warn!("init par iter {:2}/{:2}", index, peers);
            (0..(edges / peers))
                .map(move |_| ((rng1.gen_range(0..nodes), rng1.gen_range(0..nodes))))
        })
        .batch_mode(BatchMode::fixed(1024));

    let mut split = source.split(2);

    let a = split.pop().unwrap();

    let b = split.pop().unwrap().left_join(a, |x| x.0, |x| x.0).unkey();

    {
        let (a, b) = b.iterate(
            5,
            0u8,
            |s, _| s.map(|s| s),
            |_, _| (),
            |_, _: ()| (),
            |_| true,
        );
        a.for_each(std::mem::drop);
        b.for_each(std::mem::drop);
    }
    // b.collect_vec();

    env.execute();

    let elapsed = start.elapsed();

    eprintln!("Elapsed {:?}", elapsed);
}
