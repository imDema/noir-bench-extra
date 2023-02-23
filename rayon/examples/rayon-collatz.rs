use std::time::Instant;

use rayon::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    env_logger::init();
    if std::env::args().len() != 2 {
        panic!("Pass the iteration number as an argument");
    }
    let limit: u64 = std::env::args().nth(1).unwrap().parse().unwrap();
    let iter = 1000;

    let start = Instant::now();

    let result = (1..limit).into_par_iter()
        .map(|n| {
            let mut c = 0;
            let mut cur = n;
            while c < iter {
                if cur % 2 == 0 {
                    cur /= 2;
                } else {
                    cur = cur * 3 + 1;
                }
                c += 1;
                if cur <= 1 {
                    break;
                }
            }
            (c, n)
        })
        .reduce(|| (0,0), |a, b| a.max(b));
    
    let elapsed = start.elapsed();
    eprintln!("Output: {:?}", result);
    println!("{:?}", elapsed);
}
