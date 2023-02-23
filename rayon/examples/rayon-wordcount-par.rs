use std::{
    collections::HashMap,
    fs::File,
    hash::BuildHasherDefault,
    io::{BufRead, BufReader, Seek, SeekFrom},
    time::Instant,
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

    let start = Instant::now();

    let tokenizer = Tokenizer::new();

    let n: usize = std::thread::available_parallelism().unwrap().into();

    let result = (0..n)
        .into_par_iter()
        .flat_map_iter(|i| {
            let mut file = File::open(&path).unwrap();
            let file_size = file.metadata().unwrap().len() as usize;

            let range_size = file_size / n;
            let start = range_size * i;
            let mut current = start;
            let end = if i == n - 1 {
                file_size
            } else {
                start + range_size
            };

            // Seek reader to the first byte to be read
            file.seek(SeekFrom::Start(start as u64)).expect("seek file");

            let mut reader = BufReader::new(file);
            if i != 0 {
                let mut line = Vec::new();
                // discard first line
                current += reader
                    .read_until(b'\n', &mut line)
                    .expect("Cannot read line from file");
            }

            std::iter::from_fn(move || {
                if current > end {
                    return None;
                }
                let mut line = String::new();
                return match reader.read_line(&mut line) {
                    Ok(len) if len > 0 => {
                        current += len;
                        Some(line)
                    }
                    Ok(_) => None,
                    Err(e) => panic!("{:?}", e),
                };
            })
            .flat_map(|l| tokenizer.tokenize(l))
        })
        .fold(
            || HashMap::<String, u64, BuildHasherDefault<WyHash>>::default(),
            |mut map, w| {
                *map.entry(w).or_default() += 1u64;
                map
            },
        )
        .reduce(
            || HashMap::<String, u64, BuildHasherDefault<WyHash>>::default(),
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
    fn tokenize(&self, value: String) -> Vec<String> {
        self.re
            .find_iter(&value)
            .map(|t| t.as_str().to_lowercase())
            .collect()
    }
}
