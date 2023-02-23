extern crate timely;

use std::collections::HashMap;
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::time::Instant;

use regex::Regex;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::{Inspect, Map, Operator, Probe};
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
        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>(|scope| {
            input
                .to_stream(scope)
                .flat_map(move |text| tokenizer.tokenize(text))
                .unary_notify(Pipeline, "WordCount", vec![], {
                    let mut counts: HashMap<String, u64, BuildHasherDefault<WyHash>> =
                        HashMap::default();
                    let mut buf = vec![];

                    move |input, output, notificator| {
                        input.for_each(|time, data| {
                            data.swap(&mut buf);
                            for word in buf.drain(..) {
                                let entry = counts.entry(word.clone()).or_default();
                                *entry += 1;
                            }
                            notificator.notify_at(time.retain());
                        });

                        notificator.for_each(|time, _, _| {
                            let mut session = output.session(&time);
                            for (key, agg) in counts.drain() {
                                session.give((0, (key, agg)));
                            }
                        });
                    }
                })
                .aggregate(
                    |_, (word, count), r: &mut HashMap<String, u64, BuildHasherDefault<WyHash>>| {
                        *r.entry(word).or_default() += count
                    },
                    |_, r| r,
                    |_| 0,
                )
                .inspect(|q| {
                    println!("seen: {}", q.len());
                    // q.iter()
                    //     .sorted_by_key(|t| t.1)
                    //     .rev()
                    //     .take(10)
                    //     .for_each(|(k, v)| eprintln!("{:>10}:{:>10}", k, v));
                })
                .probe_with(&mut probe);
        });

        let num_replicas = worker.peers();
        let global_id = worker.index();
        let path = std::env::args().nth(1).unwrap();

        log::info!("starting w{:02} out of {:02}", global_id, num_replicas);
        log::info!("reading file '{}'", path);

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
        log::info!("w{:02} time: {:?}", global_id, elapsed);
    })
    .unwrap();

    println!("{:?}", start.elapsed());
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
