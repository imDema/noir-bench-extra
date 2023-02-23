use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read},
    time::Instant, hash::BuildHasherDefault,
};

use regex::Regex;

use rayon::prelude::*;
use wyhash::WyHash;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    env_logger::init();
    if std::env::args().len() != 2 {
        panic!("Pass the dataset path as an argument");
    }
    let path = std::env::args().nth(1).unwrap();

    let file = File::open(&path).unwrap();
    let len = file.metadata().map(|m| m.len()).unwrap_or(0) as usize;
    let start = Instant::now();
    
    let mut reader = BufReader::new(file);
    let mut text = String::with_capacity(len);
    reader.read_to_string(&mut text).unwrap();

    let tokenizer = Tokenizer::new();

    let result = text.par_lines()
        .flat_map(|l| tokenizer.tokenize(l))
        .fold(
            || HashMap::<String, u64, BuildHasherDefault<WyHash>>::default(),
            |mut map, w| {
                *map.entry(w).or_default() += 1u64;
                map
            },
        )
        .reduce(
            || HashMap::default(),
            |mut a, mut b| {
                b.drain().for_each(|(w, c)| *a.entry(w).or_default() += c);
                a
            },
        );

    let elapsed = start.elapsed();
    eprintln!("Output: {:?}", result.len());
    println!("{:?}", elapsed);
}

#[derive(Clone)]
struct Tokenizer {
    re: Regex,
}

impl Tokenizer {
    fn new() -> Self {
        Self {
            re: Regex::new(r"[A-Za-z]+").unwrap(),
        }
    }
    fn tokenize(&self, value: &str) -> Vec<String> {
        self.re
            .find_iter(&value)
            .map(|t| t.as_str().to_lowercase())
            .collect()
    }
}
