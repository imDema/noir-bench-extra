use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher, BuildHasherDefault};
use std::ops::{Add, AddAssign, Div};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use rayon::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Default)]
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
        // FIXME
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
    read_points(filename)
        .take(n)
        .collect()
}

fn read_points(filename: &str) -> impl Iterator<Item = Point> {
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
}

fn select_nearest(point: Point, centroids: &[Point]) -> Point {
    *centroids
        .iter()
        .map(|c| (c, point.distance_to(c)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .unwrap()
        .0
}

fn main() {
    if std::env::args().len() != 4 {
        panic!("Pass the number of centroid, the number of iterations and the dataset path as arguments");
    }
    let num_centroids: usize = std::env::args().nth(1).unwrap().parse().expect("Invalid number of centroids");
    let num_iters: usize = std::env::args().nth(2).unwrap().parse().expect("Invalid number of iterations");
    let path = &std::env::args().nth(3).unwrap();

    let mut centroids = read_centroids(path, num_centroids);
    assert_eq!(centroids.len(), num_centroids);

    let start = Instant::now();
    let mut points = read_points(path).map(|p| (p, centroids[0])).collect::<Vec<_>>();

    for _ in 0..num_iters {
        centroids.sort_unstable();
        let sums = points.par_iter_mut()
            .fold(|| HashMap::<Point, (Point, f64), BuildHasherDefault<wyhash::WyHash>>::default(), |mut acc, (p, centroid)| {
                *centroid = select_nearest(*p, &centroids);
                let (sum, count) = acc.entry(*centroid).or_default();
                *sum += *p;
                *count += 1f64;
                acc
            })
            .reduce(|| HashMap::default(), |mut a, mut b| {
                for (c, (s2, c2)) in b.drain() {
                    let (s1, c1) = a.entry(c).or_default();
                    *s1 += s2;
                    *c1 += c2
                }
                a
            });
        
        let mut new: Vec<Point> = sums.into_iter().map(|(_, (sum, count))| sum / count).collect();
        new.sort_unstable();

        std::mem::swap(&mut new, &mut centroids);

        if new == centroids {
            break;
        }
    }

    let elapsed = start.elapsed();
    for c in centroids {
        eprintln!("{:4.0}:{:4.0}", c.x, c.y);
    }
    eprintln!("Elapsed: {elapsed:?}");
}
