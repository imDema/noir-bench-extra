use std::collections::HashMap;
use std::time::UNIX_EPOCH;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{CapabilitySet, Map, Operator};
use timely::dataflow::{Scope, Stream};

use event::Date;

use crate::TRACK_POINT;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q5<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, usize> {
    input
        .bids(scope)
        .map(move |(t, b)| {
            (
                t,
                b.auction,
                Date::new(((*b.date_time / window_slide_ns) + 1) * window_slide_ns),
            )
        })
        // TODO: Could pre-aggregate pre-exchange, if there was reason to do so.
        .unary_frontier(
            Exchange::new(|b: &(_, usize, _)| b.1 as u64),
            "Q5 Accumulate",
            |capability, _info| {
                let mut cap_set = CapabilitySet::new();
                cap_set.insert(capability);

                let mut additions = HashMap::new();
                let mut deletions = HashMap::new();
                let mut accumulations = HashMap::new();

                let mut bids_buffer = vec![];

                move |input, output| {
                    input.for_each(|_time, data| {
                        data.swap(&mut bids_buffer);

                        for (t, auction, a_time) in bids_buffer.drain(..) {
                            additions
                                .entry(nt.from_nexmark_time(a_time))
                                .or_insert_with(Vec::new)
                                .push((t, auction));
                        }
                    });

                    // Extract and order times we can now process.
                    let mut times = {
                        let add_times = additions
                            .keys()
                            .filter(|t| !input.frontier.less_equal(t))
                            .cloned();
                        let del_times = deletions
                            .keys()
                            .filter(|t| !input.frontier.less_equal(t))
                            .cloned();
                        add_times.chain(del_times).collect::<Vec<_>>()
                    };
                    times.sort();
                    times.dedup();

                    for time in times.drain(..) {
                        if let Some(additions) = additions.remove(&time) {
                            for &(t, auction) in additions.iter() {
                                let acc = accumulations.entry(auction).or_insert((t, 0));
                                acc.0 = t.max(acc.0);
                                acc.1 += 1;
                            }
                            let new_time = time + (window_slice_count * window_slide_ns);
                            deletions.insert(new_time, additions);
                        }
                        if let Some(deletions) = deletions.remove(&time) {
                            for (_, auction) in deletions.into_iter() {
                                use std::collections::hash_map::Entry;
                                match accumulations.entry(auction) {
                                    Entry::Occupied(mut entry) => {
                                        entry.get_mut().1 -= 1;
                                        if entry.get_mut().1 == 0 {
                                            entry.remove();
                                        }
                                    }
                                    _ => panic!("entry has to exist"),
                                }
                            }
                        }
                        let time = cap_set.delayed(&time);
                        if let Some((count, auction, t)) =
                            accumulations.iter().map(|(&a, &(t, c))| (c, a, t)).max()
                        {
                            output.session(&time).give((t, auction, count));
                        }
                    }
                    cap_set.downgrade(&input.frontier.frontier());
                }
            },
        )
        .unary_frontier(
            Exchange::new(|_| 0),
            "Q5 Final Accumulate",
            |capability, _info| {
                let mut cap_set = CapabilitySet::new();
                cap_set.insert(capability);

                let mut acc = HashMap::new();

                let mut in_buffer = vec![];

                move |input, output| {
                    input.for_each(|time, data| {
                        data.swap(&mut in_buffer);

                        for (t, auction, count) in in_buffer.drain(..) {
                            let max = acc
                                .entry(*time)
                                .or_insert_with(|| (t, auction, count));

                            if count > max.2 {
                                *max = (t, auction, count);
                            }
                        }
                    });

                    let mut times = acc
                        .keys()
                        .filter(|t| !input.frontier.less_equal(t))
                        .cloned()
                        .collect::<Vec<_>>();
                    times.sort();
                    times.dedup();

                    for time in times.drain(..) {
                        let time = cap_set.delayed(&time);
                        if let Some((t, auction, count)) = acc.remove(&time)
                        {
                            output.session(&time).give((t, auction, count));
                        }
                    }
                    cap_set.downgrade(&input.frontier.frontier());
                }
            },
        )
        .map(|(t, ..)| t)
        .unary(Exchange::new(|_| 0), "collect", |_capability, _info| {
            let mut buf = Vec::new();
            move |input, _output| {
                input.for_each(|_time, t| {
                    t.swap(&mut buf);
                    buf.iter().for_each(|&t| {
                        let t = UNIX_EPOCH + t;
                        TRACK_POINT.get_or_init("q5").record(t.elapsed().unwrap())
                    });
                });
            }
        })
}
