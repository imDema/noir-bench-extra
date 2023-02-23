use std::{time::Instant, fs::File, io::BufReader, collections::HashSet};

use rayon::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    env_logger::init();
    if std::env::args().len() != 3 {
        panic!("Pass the iteration number and dataset path as an argument");
    }
    let iter = std::env::args().nth(1).unwrap().parse().unwrap();
    let path = std::env::args().nth(2).unwrap();

    let start = Instant::now();

    let file = File::open(&path).unwrap();
    let reader = BufReader::new(file);
    let csv = csv::ReaderBuilder::default().from_reader(reader);

    let mut edges = csv.into_deserialize::<(u64,u64)>()
        .map(|r| r.unwrap())
        .collect::<HashSet<(u64, u64)>>();

    let nodes: Vec<u64> = edges.iter().flat_map(|entry| [entry.0, entry.1]).collect::<HashSet<u64>>().into_iter().collect(); // Unique

    eprintln!("Starting processing");
    let mut buf = Vec::new();
    for _ in 0..iter {
        let l0 = edges.len();
        let new_edges = nodes.par_iter()
            .flat_map(|i| nodes.par_iter().map(move |j| (i, j)) )
            .flat_map_iter(|(i, j)| nodes.iter().map(move |k| (i, j, k)))
            .flat_map(|(&i, &j ,&k)| {
                if !edges.contains(&(i, j)) && edges.contains(&(i, k)) && edges.contains(&(k, j)) {
                    Some((i, j))
                } else {
                    None
                }
            });
        buf.par_extend(new_edges);
        eprintln!("{}", buf.len());
        edges.extend(buf.drain(..));

        if l0 == edges.len() {
            break;
        }
    }
    
    let elapsed = start.elapsed();
    eprintln!("Output: {:?}", edges.len());
    println!("{:?}", elapsed);
}
