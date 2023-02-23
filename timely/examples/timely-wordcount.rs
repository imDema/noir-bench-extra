use regex::Regex;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::time::Instant;
use timely::dataflow::operators::aggregation::aggregate::Aggregate;
use timely::dataflow::operators::*;
use timely::dataflow::{InputHandle, ProbeHandle};
use wyhash::WyHash;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    // initializes and runs a timely dataflow.
    env_logger::init();
    let start = Instant::now();
    timely::execute_from_args(std::env::args().skip(1), |worker| {
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        let tokenizer = Tokenizer::new();

        worker.dataflow::<usize, _, _>(|scope| {
            input
                .to_stream(scope)
                // tokenize each line
                .flat_map(move |line: String| tokenizer.tokenize(line))
                // add the counter to each word
                .map(|word| (word, 1))
                // aggregate the counts for each word, using the hash of the word for the partition
                .aggregate(
                    |_word, c, r: &mut i32| *r += c,
                    |word, count| (0, (word, count)),
                    |word| {
                        let mut hasher = WyHash::with_seed(0);
                        word.hash(&mut hasher);
                        hasher.finish()
                    },
                )
                // count the number of distinct words, for output
                .aggregate(
                    |_, v, r: &mut Vec<(String, i32)>| r.push(v),
                    |_, r| r,
                    |_| 0,
                )
                .inspect(|x| println!("Total: {} words", x.len()))
                .probe_with(&mut probe);
        });

        let num_replicas = worker.peers();
        let global_id = worker.index();
        let path = std::env::args().nth(1).unwrap();

        println!("Worker {} out of {}", global_id, num_replicas);
        println!("Reading file {}", path);

        let mut file = File::open(&path).unwrap();
        let file_size = file.metadata().unwrap().len() as usize;

        let range_size = file_size / num_replicas;
        let start = range_size * global_id;
        let mut current = start;
        let end = if global_id == num_replicas - 1 {
            file_size
        } else {
            start + range_size
        };

        // Seek reader to the first byte to be read
        file.seek(SeekFrom::Start(start as u64)).expect("seek file");

        let mut reader = BufReader::new(file);
        if global_id != 0 {
            let mut line = Vec::new();
            // discard first line
            current += reader
                .read_until(b'\n', &mut line)
                .expect("Cannot read line from file");
        }

        while current <= end {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(len) if len > 0 => {
                    current += len;
                    input.send(line);
                    worker.step();
                }
                Ok(_) => break,
                Err(e) => panic!("{:?}", e),
            }
        }

        input.advance_to(1);
        while probe.less_than(input.time()) {
            worker.step();
        }
        let elapsed = worker.timer().elapsed();
        println!("Total time: {:?}", elapsed);
    })
    .unwrap();

    println!("{:?}s", start.elapsed());
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
