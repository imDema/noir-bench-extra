use std::cmp::Ordering;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::ops::{Add, AddAssign, Div};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use noir::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
struct Point {
    x: f64,
    y: f64,
}

impl Point {
    #[allow(unused)]
    fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    #[inline(always)]
    fn distance_to(&self, other: &Point) -> f64 {
        ((self.x - other.x).powi(2) + (self.y - other.y).powi(2)).sqrt()
    }
}

impl Add for Point {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            x: self.x + other.x,
            y: self.y + other.y,
        }
    }
}

impl AddAssign for Point {
    fn add_assign(&mut self, other: Self) {
        self.x += other.x;
        self.y += other.y;
    }
}

impl PartialEq for Point {
    fn eq(&self, other: &Self) -> bool {
        let precision = 0.1;
        (self.x - other.x).abs() < precision && (self.y - other.y).abs() < precision
    }
}

impl Eq for Point {}

impl Ord for Point {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.x < other.x {
            Ordering::Less
        } else if self.x > other.x {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl PartialOrd for Point {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for Point {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.x.to_le_bytes().hash(state);
        self.y.to_le_bytes().hash(state);
    }
}

impl Div<f64> for Point {
    type Output = Self;

    fn div(self, rhs: f64) -> Self::Output {
        Self {
            x: self.x / rhs,
            y: self.y / rhs,
        }
    }
}

fn read_centroids(filename: &str, n: usize) -> Vec<Point> {
    let file = File::options()
        .read(true)
        .write(false)
        .open(filename)
        .unwrap();
    csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(file)
        .into_deserialize::<Point>()
        .map(Result::unwrap)
        .take(n)
        .collect()
}

fn select_nearest(point: Point, centroids: &[Point]) -> Point {
    *centroids
        .iter()
        .map(|c| (c, point.distance_to(c)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .unwrap()
        .0
}

#[derive(Clone, Serialize, Deserialize)]
struct State {
    iter_count: i64,
    new_centroids: Vec<Point>,
    centroids: Vec<Point>,
}

impl State {
    fn new(centroids: Vec<Point>) -> State {
        State {
            iter_count: 0,
            centroids: centroids,
            new_centroids: vec![],
        }
    }
}

fn main() {
    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 3 {
        panic!("Pass the number of centroid, the number of iterations and the dataset path as arguments");
    }
    let num_centroids: usize = args[0].parse().expect("Invalid number of centroids");
    let num_iters: usize = args[1].parse().expect("Invalid number of iterations");
    let path = &args[2];

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let centroids = read_centroids(path, num_centroids);
    assert_eq!(centroids.len(), num_centroids);
    let initial_state = State::new(centroids);

    let source = CsvSource::<Point>::new(path).has_headers(false);
    let res = env
        .stream(source)
        .replay(
            num_iters,
            initial_state,
            |s, state| {
                s.map(move |point| (point, select_nearest(point, &state.get().centroids), 1))
                    .group_by_avg(|(_p, c, _n)| *c, |(p, _c, _n)| *p)
                    .drop_key()
            },
            |update: &mut Vec<Point>, p| update.push(p),
            move |state, mut update| {
                state.new_centroids.append(&mut update);
            },
            |state| {
                state.iter_count += 1;
                state.new_centroids.sort_unstable();
                let flag = state.new_centroids != state.centroids;
                std::mem::swap(&mut state.new_centroids, &mut state.centroids);
                state.new_centroids.clear();
                flag
            },
        )
        .collect_vec();

    let start = Instant::now();
    env.execute();
    let elapsed = start.elapsed();
    if let Some(mut res) = res.get() {
        let state = res.pop().unwrap();
        for c in state.centroids {
            eprintln!("{:4.0}:{:4.0}", c.x, c.y);
        }
        eprintln!("Elapsed: {elapsed:?}");
    }
}
