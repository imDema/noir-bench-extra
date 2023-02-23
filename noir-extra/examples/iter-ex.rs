use noir::prelude::*;

fn main() {
    env_logger::init();

    // Convenience method to parse deployment config from CLI arguments
    let (config, _) = EnvironmentConfig::from_args();
    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();

    let (a, b) = env.stream_par_iter(|i, n| i..n).iterate(
        5,
        0,
        move |s, _| s.map(|q| q + 1),
        |_, _| (),
        |_, _: ()| (),
        |_| true,
    );
    a.for_each(|q| std::mem::drop(q));
    b.for_each(|q| std::mem::drop(q));

    env.execute(); // Start execution (blocking)
}
