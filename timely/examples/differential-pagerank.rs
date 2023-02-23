use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use timely::dataflow::{operators::Filter, *};
use timely::order::Product;

use differential_dataflow::input::InputSession;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{iterate::Variable, *};
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type Node = usize;
type Edge = (Node, Node);
type Time = u32;
type Iter = u32;
type Diff = isize;

fn main() {
    let max_iter: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let nodes: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(3).unwrap().parse().unwrap();

    timely::execute_from_args(std::env::args().skip(3), move |worker| {
        let timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let mut input = InputSession::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow::<Time, _, _>(|scope| {
            let edges = input.to_collection(scope);
            pagerank(max_iter, &edges)
                .consolidate()
                .inspect(|x| {
                    // println!("{}:{}", x.0, x.2);
                    core::hint::black_box(x);
                })
                .probe_with(&mut probe);
        });

        let mut rng1: SmallRng = SeedableRng::seed_from_u64(0xfeedbeef);
        let edges = (0..)
            .map(move |_| (rng1.gen_range(0..nodes), rng1.gen_range(0..nodes)))
            .skip(index)
            .step_by(peers)
            .take((edges / peers) + if index < edges % peers { 1 } else { 0 });
        
        for e in edges {
            input.update(e, 1);
        }

        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }

        eprintln!("{:?}\tinitial compute complete", timer.elapsed());
    })
    .unwrap();
}

// Returns a weighted collection in which the weight of each node is proportional
// to its PageRank in the input graph `edges`.
fn pagerank<G>(iters: Iter, edges: &Collection<G, Edge, Diff>) -> Collection<G, Node, Diff>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // initialize many surfers at each node.
    let nodes = edges
        .flat_map(|(x, y)| Some(x).into_iter().chain(Some(y)))
        .distinct();

    // snag out-degrees for each node.
    let degrs = edges.map(|(src, _dst)| src).count();

    edges.scope().iterative::<Iter, _, _>(|inner| {
        // Bring various collections into the scope.
        let edges = edges.enter(inner);
        let nodes = nodes.enter(inner);
        let degrs = degrs.enter(inner);

        // Initial and reset numbers of surfers at each node.
        let inits = nodes.explode(|node| Some((node, 6_000_000)));
        let reset = nodes.explode(|node| Some((node, 1_000_000)));

        // Define a recursive variable to track surfers.
        // We start from `inits` and cycle only `iters`.
        let ranks = Variable::new_from(inits, Product::new(Default::default(), 1));

        // Match each surfer with the degree, scale numbers down.
        let to_push = degrs
            .semijoin(&ranks)
            .threshold(|(_node, degr), rank| (5 * rank) / (6 * degr))
            .map(|(node, _degr)| node);

        // Propagate surfers along links, blend in reset surfers.
        let mut pushed = edges
            .semijoin(&to_push)
            .map(|(_node, dest)| dest)
            .concat(&reset)
            .consolidate();

        if iters > 0 {
            pushed = pushed
                .inner
                .filter(move |(_d, t, _r)| t.inner < iters)
                .as_collection();
        }

        // Bind the recursive variable, return its limit.
        ranks.set(&pushed);
        pushed.leave()
    })
}
