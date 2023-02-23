
use ::std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Filter, Operator, Map};

use ::event::{Auction, Person};

use crate::TRACK_POINT;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q3<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, (String, String, String, usize)>
{
    let auctions = input.auctions(scope)
        .filter(|(_t, a)| a.category == 10);

    let people = input.people(scope)
        .filter(|(_t, p)| p.state == "OR" || p.state == "ID" || p.state == "CA");

    let mut auctions_buffer = vec![];
    let mut people_buffer = vec![];

    auctions
        .binary(
            &people,
            Exchange::new(|(_, a): &(_, Auction)| a.seller as u64),
            Exchange::new(|(_, p): &(_, Person)| p.id as u64),
            "Q3 Join",
            |_capability, _info| {
                let mut state1 = HashMap::new();
                let mut state2 = HashMap::<usize, (Duration, Person)>::new();

                move |input1, input2, output| {

                    // Process each input auction.
                    input1.for_each(|time, data| {
                        data.swap(&mut auctions_buffer);
                        let mut session = output.session(&time);
                        for (t, auction) in auctions_buffer.drain(..) {
                            if let Some((t0, person)) = state2.get(&auction.seller) {
                                session.give((
                                    t.max(*t0),
                                    person.name.clone(),
                                    person.city.clone(),
                                    person.state.clone(),
                                    auction.id));
                            }
                            state1.entry(auction.seller).or_insert(Vec::new()).push((t, auction));
                        }
                    });

                    // Process each input person.
                    input2.for_each(|time, data| {
                        data.swap(&mut people_buffer);
                        let mut session = output.session(&time);
                        for (t, person) in people_buffer.drain(..) {
                            if let Some(auctions) = state1.get(&person.id) {
                                for (t0, auction) in auctions.iter() {
                                    session.give((
                                        t.max(*t0),
                                        person.name.clone(),
                                        person.city.clone(),
                                        person.state.clone(),
                                        auction.id));
                                }
                            }
                            state2.insert(person.id, (t, person));
                        }
                    });
                }
            }
        )
        .map(|(t, ..)| t)
        .unary(Exchange::new(|_| 0), "collect", |_capability, _info| {
            let mut buf = Vec::new();
            move |input, _output| {
                input.for_each(|_time, t| {
                    t.swap(&mut buf);
                    buf.iter().for_each(|&t| {
                        let t = UNIX_EPOCH + t;
                        TRACK_POINT.get_or_init("q3").record(t.elapsed().unwrap())
                    });
                });
            }
        })
}
