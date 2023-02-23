use rand::prelude::*;
use std::cmp::max;

use config::NEXMarkConfig;

use crate::utils::{NexmarkRng, CHANNEL_URL_MAP};

/// Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these values.
const HOT_SELLER_RATIO: usize = 100;
const HOT_AUCTION_RATIO: usize = 100;
const HOT_BIDDER_RATIO: usize = 100;


type Id = usize;
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Abomonation, Hash, Copy, Default)]
pub struct Date(usize);

impl Date {
    pub fn new(date_time: usize) -> Date {
        Date(date_time)
    }
}

impl ::std::ops::Deref for Date {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl ::std::ops::Add for Date {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Date(self.0 + other.0)
    }
}
impl ::std::ops::Sub for Date {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Date(self.0 - other.0)
    }
}

//const MIN_STRING_LENGTH: usize = 3;
// const BASE_TIME: usize = 1436918400_000;

// fn split_string_arg(string: String) -> Vec<String> {
//     string.split(",").map(String::from).collect::<Vec<String>>()
// }

#[derive(Serialize, Deserialize, Abomonation, Debug)]
struct EventCarrier {
    time: Date,
    event: Event,
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug, Abomonation)]
#[serde(tag = "type")]
pub enum Event {
    Person(Person),
    Auction(Auction),
    Bid(Bid),
}

impl Event {

    pub fn time(&self) -> Date {
        match *self {
            Event::Person(ref p) => p.date_time,
            Event::Auction(ref a) => a.date_time,
            Event::Bid(ref b) => b.date_time,
        }
    }

    pub fn create(events_so_far: usize, nex: &mut NEXMarkConfig) -> Self {
        let rem = nex.next_adjusted_event(events_so_far) % nex.proportion_denominator;
        let timestamp = Date(nex.event_timestamp_ns(nex.next_adjusted_event(events_so_far)));
        let id = nex.first_event_id + nex.next_adjusted_event(events_so_far);

        if rem < nex.person_proportion {
            Event::Person(Person::new(id, timestamp, nex))
        } else if rem < nex.person_proportion + nex.auction_proportion {
            Event::Auction(Auction::new(events_so_far, id, timestamp, nex))
        } else {
            Event::Bid(Bid::new(id, timestamp, nex))
        }
    }

    pub fn id(&self) -> Id {
        match *self {
            Event::Person(ref p) => p.id,
            Event::Auction(ref a) => a.id,
            Event::Bid(ref b) => b.auction,    // Bid eventss don't have ids, so use the associated auction id
        }
    }

    // pub fn new(events_so_far: usize, nex: &mut NEXMarkConfig) -> Self {
    //     let rem = nex.next_adjusted_event(events_so_far) % nex.proportion_denominator;
    //     let timestamp = nex.event_timestamp(nex.next_adjusted_event(events_so_far));
    //     let id = nex.first_event_id + nex.next_adjusted_event(events_so_far);

    //     let mut seed = [0u8; 32];
    //     for i in 0 .. 8 {
    //         seed[i] = ((id >> (8 * i)) & 0xFF) as u8;
    //     }

    //     let mut rng = StdRng::from_seed(seed);

    //     if rem < nex.person_proportion {
    //         Event::Person(Person::new(id, timestamp, &mut rng, nex))
    //     } else if rem < nex.person_proportion + nex.auction_proportion {
    //         Event::Auction(Auction::new(events_so_far, id, timestamp, &mut rng, nex))
    //     } else {
    //         Event::Bid(Bid::new(id, timestamp, &mut rng, nex))
    //     }
    // }
}

// impl ToData<usize, Event> for String{
//     fn to_data(self) -> Result<(usize, Event)> {
//         serde_json::from_str(&self)
//             .map(|c: EventCarrier| (c.time, c.event))
//             .map_err(|e| e.into())
//     }
// }

// impl FromData<usize> for Event {
//     fn from_data(&self, t: &usize) -> String {
//         serde_json::to_string(&EventCarrier{ time: t.clone(), event: self.clone()}).unwrap()
//     }
// }

#[derive(Eq, PartialEq,  Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Abomonation, Hash)]
pub struct Person{
    pub id: Id,
    pub name: String,
    pub email_address: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: Date,
    pub extra: String,
}

impl Person {
    pub fn from(event: Event) -> Option<Person> {
        match event {
            Event::Person(p) => Some(p),
            _ => None
        }
    }

    fn new(id: usize, time: Date, cfg: &NEXMarkConfig) -> Self {
        let rng = &mut SmallRng::seed_from_u64(id as u64);
        let id = Self::last_id(id, cfg) + cfg.first_person_id;
        let name = format!(
            "{} {}",
            cfg.first_names.choose(rng).unwrap(),
            cfg.last_names.choose(rng).unwrap(),
        );
        let email_address = format!("{}@{}.com", rng.gen_string(7), rng.gen_string(5));
        let credit_card = format!(
            "{:04} {:04} {:04} {:04}",
            rng.gen_range(0..10000),
            rng.gen_range(0..10000),
            rng.gen_range(0..10000),
            rng.gen_range(0..10000)
        );
        let city = cfg.us_cities.choose(rng).unwrap().clone();
        let state = cfg.us_states.choose(rng).unwrap().clone();

        let current_size =
            8 + name.len() + email_address.len() + credit_card.len() + city.len() + state.len();
        let extra = rng.gen_next_extra(current_size, cfg.avg_person_byte_size);

        Self {
            id,
            name,
            email_address,
            credit_card,
            city,
            state,
            date_time: time,
            extra,
        }
    }

    /// Return a random person id (base 0).
    fn next_id(event_id: usize, rng: &mut SmallRng, nex: &NEXMarkConfig) -> Id {
        let people = Self::last_id(event_id, nex) + 1;
        let active = people.min(nex.active_people);
        people - active + rng.gen_range(0..active + nex.person_id_lead)
    }

    /// Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the current person id if
    /// due to generate a person.
    fn last_id(event_id: usize, nex: &NEXMarkConfig) -> Id {
        let epoch = event_id / nex.proportion_denominator;
        let offset = (event_id % nex.proportion_denominator).min(nex.person_proportion - 1);
        epoch * nex.person_proportion + offset
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Abomonation, Hash)]
pub struct Auction{
    pub id: Id,
    pub item_name: String,
    pub description: String,
    pub initial_bid: usize,
    pub reserve: usize,
    pub date_time: Date,
    pub expires: Date,
    pub seller: Id,
    pub category: Id,
    pub extra: String,
}
// unsafe_abomonate!(Auction : id, item_name, description, initial_bid, reserve, date_time, expires, seller, category);

impl Auction {
    pub fn from(event: Event) -> Option<Auction> {
        match event {
            Event::Auction(p) => Some(p),
            _ => None
        }
    }

    fn new(event_number: usize,
        event_id: usize,
        time: Date, cfg: &NEXMarkConfig) -> Self {
        let rng = &mut SmallRng::seed_from_u64(event_id as u64);
        let id = Self::last_id(event_id, cfg) + cfg.first_auction_id;
        let item_name = rng.gen_string(20);
        let description = rng.gen_string(100);
        let initial_bid = rng.gen_price();

        let reserve = initial_bid + rng.gen_price();
        let expires = time + Self::next_length(event_number, rng, time, cfg);

        // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
        let mut seller = if rng.gen_range(0..cfg.hot_seller_ratio) > 0 {
            // Choose the first person in the batch of last HOT_SELLER_RATIO people.
            (Person::last_id(event_id, cfg) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO
        } else {
            Person::next_id(event_id, rng, cfg)
        };
        seller += cfg.first_person_id;
        let category = cfg.first_category_id + rng.gen_range(0..cfg.num_categories);

        let current_size = 8 + item_name.len() + description.len() + 8 + 8 + 8 + 8 + 8;
        let extra = rng.gen_next_extra(current_size, cfg.avg_auction_byte_size);

        Auction {
            id,
            item_name,
            description,
            initial_bid,
            reserve,
            date_time: time,
            expires,
            seller,
            category,
            extra,
        }
    }

    fn next_id(id: usize, rng: &mut SmallRng, nex: &NEXMarkConfig) -> Id {
        let max_auction = Self::last_id(id, nex);
        let min_auction = if max_auction < nex.in_flight_auctions { 0 } else { max_auction - nex.in_flight_auctions };
        min_auction + rng.gen_range(0..(max_auction - min_auction + 1 + nex.auction_id_lead))
    }

    fn last_id(id: usize, nex: &NEXMarkConfig) -> Id {
        let mut epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if offset < nex.person_proportion {
            epoch -= 1;
            offset = nex.auction_proportion - 1;
        } else if nex.person_proportion + nex.auction_proportion <= offset {
            offset = nex.auction_proportion - 1;
        } else {
            offset -= nex.person_proportion;
        }
        epoch * nex.auction_proportion + offset
    }

    fn next_length(events_so_far: usize, rng: &mut SmallRng, time: Date, nex: &NEXMarkConfig) -> Date {
        let current_event = nex.next_adjusted_event(events_so_far);
        let events_for_auctions = (nex.in_flight_auctions * nex.proportion_denominator) / nex.auction_proportion;
        let future_auction = nex.event_timestamp_ns(current_event+events_for_auctions);

        let horizon = future_auction - time.0;
        Date(1 + rng.gen_range(0..max(horizon * 2, 1)))
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Abomonation, Hash)]
pub struct Bid {
    /// The ID of the auction this bid is for.
    pub auction: Id,
    /// The ID of the person that placed this bid.
    pub bidder: Id,
    /// The price in cents that the person bid for.
    pub price: usize,
    /// The channel of this bid
    pub channel: String,
    /// The url of this bid
    pub url: String,
    /// A millisecond timestamp for the event origin.
    pub date_time: Date,
    /// Extra information
    pub extra: String,
}
// unsafe_abomonate!(Bid : auction, bidder, price, date_time);

impl Bid {
    pub fn from(event: Event) -> Option<Bid> {
        match event {
            Event::Bid(p) => Some(p),
            _ => None
        }
    }

    pub(crate) fn new(event_id: usize, time: Date, nex: &NEXMarkConfig) -> Self {
        let rng = &mut SmallRng::seed_from_u64(event_id as u64);
        // Here P(bid will be for a hot auction) = 1 - 1/hotAuctionRatio.
        let auction = if 0 < rng.gen_range(0..nex.hot_auction_ratio) {
            // Choose the first auction in the batch of last HOT_AUCTION_RATIO auctions.
            (Auction::last_id(event_id, nex) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO
        } else {
            Auction::next_id(event_id, rng, nex)
        };

        // Here P(bid will be by a hot bidder) = 1 - 1/hotBiddersRatio
        let bidder = if 0 < rng.gen_range(0..nex.hot_bidder_ratio) {
            // Choose the second person (so hot bidders and hot sellers don't collide) in the batch of
            // last HOT_BIDDER_RATIO people.
            (Person::last_id(event_id, nex) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1
        } else {
            Person::next_id(event_id, rng, nex)
        };

        let price = rng.gen_price();

        let (channel, url) = if rng.gen_range(0..nex.hot_channel_ratio) > 0 {
            let index = rng.gen_range(0..nex.hot_channels.len());
            (nex.hot_channels[index].clone(), nex.hot_urls[index].clone())
        } else {
            CHANNEL_URL_MAP.choose(rng).unwrap().clone()
        };

        let current_size = 8 + 8 + 8 + 8;
        let extra = rng.gen_next_extra(current_size, nex.avg_bid_byte_size);

        Bid {
            auction: auction + nex.first_auction_id,
            bidder: bidder + nex.first_person_id,
            price,
            date_time: time,
            channel,
            url,
            extra,
        }
    }
}
