use std::time::UNIX_EPOCH;

use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::{operators::{Filter, Map, Operator}, channels::pact::{Pipeline}};

use crate::TRACK_POINT;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q2<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, (usize, usize)>
{
    let auction_skip = 123;
    input.bids(scope)
        .filter(move |(_t, b)| b.auction % auction_skip == 0)
        .map(|(t, b)| (t, b.auction, b.price))
        .map(|(t, ..)| t)
        .unary(Pipeline, "collect", |_capability, _info| {
            let mut buf = Vec::new();
            move |input, _output| {
                input.for_each(|_time, t| {
                    t.swap(&mut buf);
                    buf.iter().for_each(|&t| {
                        let t = UNIX_EPOCH + t;
                        TRACK_POINT.get_or_init("q2").record(t.elapsed().unwrap())
                    });
                });
            }
        })
}
