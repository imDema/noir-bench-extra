use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

use rand::{rngs::SmallRng, Rng, SeedableRng};

use noir::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const INIT: i64 = 6_000_000;
const HOP: i64 = 1_000_000;

fn main() {
    let start = Instant::now();
    env_logger::init();
    let (config, args) = EnvironmentConfig::from_args();

    // command-line args: numbers of nodes and edges in the random graph.
    let nodes: u64 = args[0].parse().unwrap();
    let edges: u64 = args[1].parse().unwrap();

    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();

    let source = env
        .stream(ParallelIteratorSource::new(move |index, peers| {
            let mut rng1: SmallRng = SeedableRng::seed_from_u64(index as u64);
            (0..(edges / peers))
                .map(move |_| ((rng1.gen_range(0..nodes), rng1.gen_range(0..nodes))))
        }))
        .batch_mode(BatchMode::fixed(8196));

    let mut split = source.split(2);

    let adj_list = split
        .pop()
        .unwrap()
        // construct adjacency list
        .group_by_fold(
            |(x, _y)| *x,
            Vec::new(),
            |edges, (_x, y)| edges.push(y),
            |edges1, edges2| edges1.extend(edges2),
        );

    let init = split
        .pop()
        .unwrap()
        .flat_map(|(x, y)| [x, y])
        .group_by_fold(|x| *x, (), |_, _| (), |_, _| ())
        .unkey()
        .map(|(x, ())| (x, (INIT, true)));

    static I: AtomicUsize = AtomicUsize::new(0);
    let (state, out) = init.iterate(
        10000,
        false,
        {
            move |s, _| {
                s.to_keyed()
                    .join(adj_list)
                    .flat_map(move |(x, ((rank, _), edges))| {
                        let mut v = Vec::with_capacity(edges.len() + 1);

                        if !edges.is_empty() {
                            let degree = edges.len() as i64;
                            let share = (rank * 5) / (6 * degree);
                            for i in 0..edges.len() {
                                v.push((edges[i], share));
                            }
                        }

                        v.push((*x, HOP));
                        v
                    })
                    .drop_key()
                    .group_by_sum(|x| x.0, |x| x.1)
                    .rich_map({
                        let mut prev = 0;
                        move |(_, rank)| {
                            let changed = prev != rank;
                            prev = rank;
                            (rank, changed)
                        }
                    })
                    .unkey()
            }
        },
        |changed: &mut bool, x| *changed |= x.1 .1,
        |global, local| *global |= local,
        |s| {
            I.fetch_add(1, Ordering::Relaxed);
            let b = *s;
            *s = false;
            b
        },
    );

    out.for_each(|x| println!("({},0,{})", x.0, x.1 .0));
    state.for_each(std::mem::drop);

    env.execute();

    // if let Some(mut v) = res.get() {
    //     v.sort_unstable_by(|&a, &b| b.2.partial_cmp(&a.2).unwrap());
    //     v.iter().take(3).for_each(|x| println!("{:2}: {:3}", x.0, x.2));
    //     println!("...");
    //     v.iter().rev().take(3).rev().for_each(|x| println!("{:2}: {:3}", x.0, x.2));
    //     println!("Avg: {}", v.iter().map(|x| x.1).sum::<i64>() / nodes as i64);
    //     eprintln!();
    eprintln!("Iters {:4}", I.load(Ordering::Relaxed));
    // }
    let elapsed = start.elapsed();

    eprintln!("Elapsed {:?}", elapsed);
}
