//! NEXMark benchmark implementation.
//!
//! Query parameters based on http://datalab.cs.pdx.edu/niagara/NEXMark/ unless noted otherwise.

extern crate clap;
extern crate fnv;
extern crate rand;
extern crate timely;
extern crate nexmark;
extern crate streaming_harness;
extern crate dynamic_scaling_mechanism;
extern crate abomonation;

// #[global_allocator]
// static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// use std::hash::Hash;
// use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::time::{Instant, Duration, SystemTime, UNIX_EPOCH};

use clap::{Arg, App};

use streaming_harness::util::ToNanos;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Probe, Capture};

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::Broadcast;
use timely::dataflow::Stream;
use timely::dataflow::Scope;
use timely::ExchangeData;

use dynamic_scaling_mechanism::{ControlInst, Control};

use nexmark::event::Event;
use nexmark::tools::ExperimentMapMode;
use nexmark::queries::{NexmarkInput, NexmarkTimer};

// fn calculate_hash<T: Hash>(t: &T) -> u64 {
//     let mut h: ::fnv::FnvHasher = Default::default();
//     t.hash(&mut h);
//     h.finish()
// }

#[allow(dead_code)]
fn verify<S: Scope, T: ExchangeData+Ord+::std::fmt::Debug>(correct: &Stream<S, T>, output: &Stream<S, T>) -> Stream<S, ()> {
    use timely::dataflow::channels::pact::Exchange;
    use std::collections::HashMap;
    let mut in1_pending: HashMap<_, Vec<_>> = Default::default();
    let mut in2_pending: HashMap<_, Vec<_>> = Default::default();
    let mut data_buffer: Vec<T> = Vec::new();
    correct.binary_notify(&output, Exchange::new(|_| 0), Exchange::new(|_| 0), "Verify", vec![],
        move |in1, in2, _out, not| {
            in1.for_each(|time, data| {
                data.swap(&mut data_buffer);
                in1_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data_buffer.drain(..));
                not.notify_at(time.retain());
            });
            in2.for_each(|time, data| {
                data.swap(&mut data_buffer);
                in2_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data_buffer.drain(..));
                not.notify_at(time.retain());
            });
            not.for_each(|time, _, _| {
                let mut v1 = in1_pending.remove(time.time()).unwrap_or_default();
                let mut v2 = in2_pending.remove(time.time()).unwrap_or_default();
                v1.sort();
                v2.sort();
                assert_eq!(v1.len(), v2.len());
                let i1 = v1.iter();
                let i2 = v2.iter();
                for (a, b) in i1.zip(i2) {
//                    println!("a: {:?}, b: {:?}", a, b);
                    assert_eq!(a, b, " at {:?}", time.time());
                }
            })
        }
    )
}

fn main() {

    let matches = App::new("word_count")
        .arg(Arg::with_name("rate").long("rate").takes_value(true).required(true))
        .arg(Arg::with_name("duration").long("duration").takes_value(true).required(true))
        .arg(Arg::with_name("migration").long("migration").takes_value(true).required(true))
        .arg(Arg::with_name("time_dilation").long("time_dilation").takes_value(true).required(false))
        .arg(Arg::with_name("queries").long("queries").takes_value(true).required(true).multiple(true).value_delimiter(" "))
        .arg(Arg::with_name("dt_us").long("dt-us").takes_value(true).required(false).default_value("100000"))
        .arg(Arg::with_name("timely").multiple(true))
        .get_matches();
    let timely_args = matches.values_of("timely").map_or(Vec::new(), |vs| vs.map(String::from).collect());


    let rate: u64 = matches.value_of("rate").expect("rate absent").parse::<u64>().expect("couldn't parse rate");

    let duration_ns: u64 = matches.value_of("duration").expect("duration absent").parse::<u64>().expect("couldn't parse duration") * 1_000_000_000;

    let dt_ns: u64 = matches.value_of("dt_us").expect("dt_us missing").parse::<u64>().expect("couldn't parse dt_us") * 1_000;

    let map_mode: ExperimentMapMode = matches.value_of("migration").expect("migration file absent").parse().unwrap();

    let time_dilation = matches.value_of("time_dilation").map_or(1, |arg| arg.parse().unwrap_or(1));

    let queries: Vec<_> = matches.values_of("queries").unwrap().map(String::from).collect();

    assert_eq!(queries.len(), 1);
    let q = &queries[0].to_owned();
    static FIRST: AtomicBool = AtomicBool::new(false);

    // Read and report RSS
    // let statm_reporter_running = nexmark::tools::statm_reporter();

    // define a new computational scope, in which to run NEXMark queries
    let start = Instant::now();
    let _timelines: Vec<_> = timely::execute_from_args(timely_args.into_iter(), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        if index == 0 {
            FIRST.store(true, std::sync::atomic::Ordering::Relaxed);
        }

        // Declare re-used input, control and probe handles.
        let mut input = InputHandle::<_, (Duration, Event)>::new();
        let mut control_input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        {
            let control = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());

            let bids = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());
            let auctions = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());
            let people = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());


            let closed_auctions = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());
            let closed_auctions_flex = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());

            let nexmark_input = NexmarkInput {
                control: &control,
                bids: &bids,
                auctions: &auctions,
                people: &people,
                closed_auctions: &closed_auctions,
                closed_auctions_flex: &closed_auctions_flex,
            };

            let nexmark_timer = NexmarkTimer {
                time_dilation
            };

            eprintln!("starting {index:02}[{peers:02}]");

            worker.dataflow(|scope: &mut ::timely::dataflow::scopes::Child<_, usize>| {
                use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
                let mut demux = OperatorBuilder::new("NEXMark demux".to_string(), scope.clone());

                let mut input = demux.new_input(&input.to_stream(scope), Pipeline);

                let (mut b_out, bids_stream) = demux.new_output();
                let (mut a_out, auctions_stream) = demux.new_output();
                let (mut p_out, people_stream) = demux.new_output();

                let mut demux_buffer = Vec::new();

                demux.build(move |_capability| {
                    move |_frontiers| {
                        let mut b_out = b_out.activate();
                        let mut a_out = a_out.activate();
                        let mut p_out = p_out.activate();

                        input.for_each(|time, data| {
                            data.swap(&mut demux_buffer);
                            let mut b_session = b_out.session(&time);
                            let mut a_session = a_out.session(&time);
                            let mut p_session = p_out.session(&time);

                            for (t, datum) in demux_buffer.drain(..) {
                                match datum {
                                    nexmark::event::Event::Bid(b) => { b_session.give((t, b)) },
                                    nexmark::event::Event::Auction(a) => { a_session.give((t, a)) },
                                    nexmark::event::Event::Person(p) => { p_session.give((t, p)) },
                                }
                            }
                        });
                    }
                });

                bids_stream.capture_into(bids.clone());
                auctions_stream.capture_into(auctions.clone());
                people_stream.capture_into(people.clone());
                control_input.to_stream(scope).broadcast().capture_into(control.clone());
            });

            // Q2: Filter some auctions.
            if queries.iter().any(|x| *x == "q2") {
                worker.dataflow(|scope| {
                    ::nexmark::queries::q2(&nexmark_input, nexmark_timer, scope).probe_with(&mut probe);
                });
            }

            // Q3: Join some auctions.
            if queries.iter().any(|x| *x == "q3") {
                worker.dataflow(|scope| {
                    ::nexmark::queries::q3(&nexmark_input, nexmark_timer, scope).probe_with(&mut probe);
                });
            }

            if queries.iter().any(|x| *x == "q5") {
                // 60s windows, ticking in 1s intervals
                // NEXMark default is 60 minutes, ticking in one minute intervals
                let window_size_ns = 1_000_000_000;
                let window_slide_ns = 100_000_000;
                let window_slice_count = window_size_ns / window_slide_ns;
                worker.dataflow(|scope| {
                    ::nexmark::queries::q5(&nexmark_input, nexmark_timer, scope, window_slice_count, window_slide_ns).probe_with(&mut probe);
                });
            }

        }

        let mut config1 = nexmark::config::Config::new();
        // 0.06*60*60*12 = 0.06*60*60*12
        // auction_proportion*sec_in_12h
        // config1.insert("in-flight-auctions", format!("{}", rate * 2592));
        config1.insert("events-per-second", format!("{}", rate));
        config1.insert("first-event-number", format!("{}", index));
        let mut config = nexmark::config::NEXMarkConfig::new(&config1);

        let mut instructions: Vec<(u64, Vec<ControlInst>)> = map_mode.instructions(peers, duration_ns).unwrap();

        if index == 0 {
            println!("time_dilation\t{}", time_dilation);
            println!("bin_shift\t{}", ::dynamic_scaling_mechanism::BIN_SHIFT);

            for instruction in instructions.iter().take(10) {
                // Format instructions first to be able to truncate the string representation
                eprintln!("instructions\t{:.120}", format!("{:?}", instruction));
            }
        }

        let mut control_sequence = 0;
        let mut control_input = Some(control_input);
        if index != 0 {
            control_input.take().unwrap().close();
        } else {
            let control_input = control_input.as_mut().unwrap();
            if instructions.get(0).map_or(false, |(ts, _)| *ts == 0) {
                let (_ts, ctrl_instructions) = instructions.remove(0);
                let count = ctrl_instructions.len();

                for instruction in ctrl_instructions {
                    control_input.send(Control::new(control_sequence, count, instruction));
                }
                control_sequence += 1;
            }
            control_input.advance_to(1);
        }

        let count = 1;
        input.advance_to(count);
        while probe.less_than(&count) { worker.step(); }

        let timer = ::std::time::Instant::now();

        // Establish a start of the computation.
        let elapsed_ns = timer.elapsed().to_nanos();
        config.base_time_ns = elapsed_ns as usize;

        assert!(worker.peers() < 256);

        let input_times = {
            let config = config.clone();
            // We generate only from one peer to measure latency correctly
            move || nexmark::config::NexMarkInputTimes::new(config.clone(), duration_ns, time_dilation, 1)
        };

        let mut output_metric_collector =
            ::streaming_harness::output::default::hdrhist_timeline_collector(
                input_times(),
                0, 1_000_000_000, duration_ns - 1_000_000_000, duration_ns,
                250_000_000);

        let mut events_so_far = 0;

        let mut input_times_gen =
            ::streaming_harness::input::SyntheticInputTimeGenerator::new(input_times());

        let mut input = Some(input);

        let mut last_migrated = None;

        let mut last_ns = 0;

        loop {
            let elapsed_ns = last_ns + 1_000_000;
            let wait_ns = last_ns;
            let target_ns = elapsed_ns;
            last_ns = target_ns;

            if index == 0 {
//                ::std::thread::sleep(::std::time::Duration::from_millis(10));
                if let Some(control_input) = control_input.as_mut() {
                    if last_migrated.map_or(true, |time| *control_input.time() != time)
                        && instructions.get(0).map(|&(ts, _)| ts as usize + count <= *control_input.time()).unwrap_or(false)
                        {
                            let (ts, ctrl_instructions) = instructions.remove(0);
                            let count = ctrl_instructions.len();

                            if *control_input.time() < ts as usize + count {
                                control_input.advance_to(ts as usize + count);
                            }

                            println!("control_time\t{}", control_input.time());

                            for instruction in ctrl_instructions {
                                control_input.send(Control::new(control_sequence, count, instruction));
                            }

                            control_sequence += 1;
                            last_migrated = Some(*control_input.time());
                        }
                }

                if instructions.is_empty() {
                    control_input.take();
                }
            }

            output_metric_collector.acknowledge_while(
                elapsed_ns,
                |t| {
                    !probe.less_than(&(t as usize + count))
                });

            if input.is_none() {
                break;
            }

            if index == 0 {
                if let Some(it) = input_times_gen.iter_until(target_ns) {
                    let input = input.as_mut().unwrap();
                    for _t in it {
                        let e = Event::create(
                            events_so_far,
                            &mut config);
                        input.send((SystemTime::now().duration_since(UNIX_EPOCH).unwrap(), e));
                        events_so_far += 1;
                    }
                    input.advance_to(target_ns as usize + count);
                    if let Some(control_input) = control_input.as_mut() {
                        if *control_input.time() < target_ns as usize + count {
                            control_input.advance_to(target_ns as usize + count);
                        }
                    }
                } else {
                    input.take().unwrap();
                    control_input.take();
                }
            } else {
                input.take().unwrap();
                control_input.take();
            }

            if input.is_some() {
                while probe.less_than(&(wait_ns as usize + count)) { worker.step(); }
            } else {
                while worker.step() { }
            }
        }

        output_metric_collector.into_inner()
    }).expect("unsuccessful execution").join().into_iter().map(|x| x.unwrap()).collect();

    // statm_reporter_running.store(false, ::std::sync::atomic::Ordering::SeqCst);

    // let ::streaming_harness::timeline::Timeline { timeline, latency_metrics, .. } = ::streaming_harness::output::combine_all(timelines);

    // let latency_metrics = latency_metrics.into_inner();
    // println!("DEBUG_summary\t{}", latency_metrics.summary_string().replace("\n", "\nDEBUG_summary\t"));
    // println!("{}",
    //           timeline.clone().into_iter().map(|::streaming_harness::timeline::TimelineElement { time, metrics, samples }|
    //                 format!("DEBUG_timeline\t-- {} ({} samples) --\nDEBUG_timeline\t{}", time, samples, metrics.summary_string().replace("\n", "\nDEBUG_timeline\t"))).collect::<Vec<_>>().join("\n"));

    // for (value, prob, count) in latency_metrics.ccdf() {
    //     println!("latency_ccdf\t{}\t{}\t{}", value, prob, count);
    // }
    // println!("{}", ::streaming_harness::format::format_summary_timeline("summary_timeline".to_string(), timeline.clone()));
    if FIRST.load(std::sync::atomic::Ordering::Relaxed) {
        println!("{q}:elapsed:{:?}", start.elapsed());

        micrometer::summary_grouped();
        micrometer::append_csv("/tmp/nexmark-latency.csv", "timely").unwrap();    
    }
}
