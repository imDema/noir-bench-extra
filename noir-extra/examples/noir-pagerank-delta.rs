use std::{ops::AddAssign, time::Instant};

use rand::{rngs::SmallRng, Rng, SeedableRng};

use noir::{prelude::*, IterationStateHandle};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
enum Msg {
    Init { rank_init: i64, adj: Vec<u64> },
    Delta { delta_rank: i64 },
    Output { rank: i64 },
}

impl Msg {
    fn delta(delta_rank: i64) -> Self {
        Self::Delta { delta_rank }
    }

    fn rank(&self) -> i64 {
        match self {
            Msg::Init { rank_init, .. } => *rank_init,
            Msg::Delta { delta_rank } => *delta_rank,
            Msg::Output { rank } => *rank,
        }
    }
}

impl AddAssign<Self> for Msg {
    fn add_assign(&mut self, rhs: Self) {
        match (self, rhs) {
            (Msg::Delta { delta_rank: a }, Msg::Delta { delta_rank: b }) => *a += b,
            _ => panic!("Summing incompatible Msg"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
struct TerminationCond {
    something_changed: bool,
    last_iteration: bool,
    iter: usize,
}

#[derive(Clone)]
struct Node {
    max_iter: usize,
    rank: i64,
    adj_list: Vec<u64>,
}

impl Node {
    fn new(max_iter: usize) -> Self {
        Self {
            max_iter,
            rank: Default::default(),
            adj_list: Default::default(),
        }
    }

    fn process_msg(&mut self, state: &TerminationCond, x: &u64, msg: Msg) -> Vec<(u64, Msg)> {
        if state.last_iteration || state.iter == self.max_iter - 2 {
            return vec![(*x, Msg::Output { rank: self.rank })];
        }

        match msg {
            Msg::Init { rank_init, adj } => {
                self.adj_list = adj;
                self.rank = 0;

                vec![(*x, Msg::delta(rank_init))]
            }
            Msg::Delta { delta_rank } => {
                self.rank += delta_rank;
                let mut update = Vec::with_capacity(self.adj_list.len() + 1);

                if !self.adj_list.is_empty() {
                    let degree = self.adj_list.len() as i64;
                    let new_share = (self.rank * 5) / (6 * degree);
                    for i in 0..self.adj_list.len() {
                        update.push((self.adj_list[i], Msg::delta(new_share)));
                    }
                }

                update.push((*x, Msg::delta(HOP - self.rank)));
                update
            }
            Msg::Output { .. } => unreachable!("should never have output here"),
        }
    }
}

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const INIT: i64 = 6_000_000;
const HOP: i64 = 1_000_000;

fn main() {
    env_logger::init();

    let start = Instant::now();
    let (config, args) = EnvironmentConfig::from_args();

    // command-line args: numbers of nodes and edges in the random graph.
    let max_iter: usize = args[0].parse().unwrap();
    let nodes: u64 = args[1].parse().unwrap();
    let edges: u64 = args[2].parse().unwrap();

    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();

    let source = env
        .stream_par_iter(move |index, peers| {
            let mut rng1: SmallRng = SeedableRng::seed_from_u64(0xfeedbeef);
            (0..)
                .map(move |_| (rng1.gen_range(0..nodes), rng1.gen_range(0..nodes)))
                .skip(index as usize)
                .step_by(peers as usize)
                .take((edges / peers) as usize + if index < edges % peers { 1 } else { 0 })
        })
        .batch_mode(BatchMode::fixed(1024));

    let mut split = source.split(2);

    let adj_list = split
        .pop()
        .unwrap()
        .group_by(|(x, _y)| *x)
        .fold(Vec::new(), |edges, (_x, y)| edges.push(y))
        .unkey();

    let init = split
        .pop()
        .unwrap()
        .flat_map(|(x, y)| [x, y])
        .group_by_fold(|x| *x, (), |_, _| (), |_, _| ())
        .unkey()
        .left_join(adj_list, |x| x.0, |x| x.0)
        .map(|(_, (_, vec))| Msg::Init {
            rank_init: INIT,
            adj: vec.map(|(_, v)| v).unwrap_or_default(),
        })
        .unkey();

    let (state, out) = init.iterate(
        max_iter,
        TerminationCond {
            something_changed: false,
            last_iteration: false,
            iter: 0,
        },
        move |s, state: IterationStateHandle<TerminationCond>| {
            s.to_keyed()
                .rich_flat_map({
                    let mut node = Node::new(max_iter);
                    move |(x, msg): (_, Msg)| node.process_msg(state.get(), x, msg)
                })
                .drop_key()
                .group_by_sum(|x| x.0, |x| x.1)
                .unkey()
        },
        |changed: &mut TerminationCond, x| {
            changed.something_changed = !matches!(x.1, Msg::Delta { delta_rank: 0 });
            if let Msg::Output { .. } = x.1 {
                changed.last_iteration = true;
            }
        },
        |global, local| {
            global.something_changed |= local.something_changed;
            global.last_iteration |= local.last_iteration;
        },
        |s| {
            let cond = !s.last_iteration;
            if !s.something_changed {
                s.last_iteration = true;
            }
            s.something_changed = false;
            s.iter += 1;
            cond
        },
    );

    out.for_each(|x| {
        // println!("{}:{}", x.0, x.1.rank(),);
        core::hint::black_box(x);
    });
    state.for_each(std::mem::drop);

    env.execute();

    let elapsed = start.elapsed();

    eprintln!("Elapsed {:?}", elapsed);
}
