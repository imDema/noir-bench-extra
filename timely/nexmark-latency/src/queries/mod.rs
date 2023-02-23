use ::std::rc::Rc;
use std::time::Duration;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;

use dynamic_scaling_mechanism::Control;
use event::{Bid, Auction, Person, Date};

mod q2;
mod q3;
mod q5;

pub use self::q2::q2;
pub use self::q3::q3;
pub use self::q5::q5;

pub struct NexmarkInput<'a> {
    pub control: &'a Rc<EventLink<usize, Control>>,
    pub bids: &'a Rc<EventLink<usize, (Duration, Bid)>>,
    pub auctions: &'a Rc<EventLink<usize, (Duration, Auction)>>,
    pub people: &'a Rc<EventLink<usize, (Duration, Person)>>,
    pub closed_auctions: &'a Rc<EventLink<usize, (Auction, Bid)>>,
    pub closed_auctions_flex: &'a Rc<EventLink<usize, (Auction, Bid)>>,
}

impl<'a> NexmarkInput<'a> {
    pub fn control<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, Control> {
        Some(self.control.clone()).replay_into(scope)
    }

    pub fn bids<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, (Duration, Bid)> {
        Some(self.bids.clone()).replay_into(scope)
    }

    pub fn auctions<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, (Duration, Auction)> {
        Some(self.auctions.clone()).replay_into(scope)
    }

    pub fn people<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, (Duration, Person)> {
        Some(self.people.clone()).replay_into(scope)
    }

    pub fn closed_auctions<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, (Auction, Bid)> {
        Some(self.closed_auctions.clone()).replay_into(scope)
    }

    pub fn closed_auctions_flex<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, (Auction, Bid)> {
        Some(self.closed_auctions_flex.clone()).replay_into(scope)
    }
}


#[derive(Copy, Clone)]
pub struct NexmarkTimer {
    pub time_dilation: usize
}

impl NexmarkTimer {
    #[inline(always)]
    fn from_nexmark_time(self, x: Date) -> usize{
        *x / self.time_dilation
    }

}
