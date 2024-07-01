//! Server implementation

#![warn(missing_docs)]

mod http;
pub mod slug;

use std::thread;

use ticket_sale_core::{Config, RequestHandler};

/// Command line options
#[derive(Debug)]
struct Opts {
    /// Configuration of the ticket sales system
    config: Config,

    /// Port for the HTTP server to listen on
    port: u16,
    /// Port for the HTTP server to listen on
    host: String,
    /// Number of load balancer threads
    balancer_threads: u32,

    /// Run the sequential “slug” implementation 🐌
    slug: bool,
}

impl Opts {
    fn from_args() -> Self {
        let mut opts = Opts {
            port: 8585,
            host: String::from("127.0.0.1"),
            config: Config {
                tickets: 1000,
                timeout: 10,
                initial_servers: 2,
                estimator_roundtrip_time: 10,
                bonus: false,
            },
            balancer_threads: 64,
            slug: false,
        };

        let mut option: Option<String> = None;
        for arg in std::env::args().skip(1) {
            if let Some(opt) = option {
                match opt.as_str() {
                    "-port" => opts.port = arg.parse().expect("-port takes a decimal u16"),
                    "-host" => opts.host = arg,
                    "-tickets" => {
                        opts.config.tickets = arg.parse().expect("-tickets takes a decimal u32")
                    }
                    "-balancer-threads" => {
                        opts.balancer_threads =
                            arg.parse().expect("-balancer-threads takes a decimal u32")
                    }
                    "-timeout" => {
                        opts.config.timeout = arg.parse().expect("-timeout takes a decimal u32")
                    }
                    "-estimator-roundtrip-time" => {
                        opts.config.estimator_roundtrip_time = arg
                            .parse()
                            .expect("-estimator-roundtrip-time takes a decimal u32")
                    }
                    _ => {
                        eprintln!("Error: ignoring unknown option {opt}");
                        std::process::exit(1);
                    }
                }
                option = None;
            } else {
                match arg.as_str() {
                    "-bonus" => opts.config.bonus = true,
                    "-slug" => opts.slug = true,
                    _ => option = Some(arg),
                }
            }
        }
        if let Some(opt) = option {
            eprintln!("Error: ignoring leftover option {opt}");
            std::process::exit(1);
        }

        opts
    }
}

fn http_loop<H: RequestHandler>(server: &tiny_http::Server, handler: &H) {
    loop {
        let rq = server.recv().expect("HTTP receive failed");
        if let Some(rq) = http::parse(rq) {
            handler.handle(rq);
        }
    }
}

fn main() {
    let opts = Opts::from_args();

    let server = tiny_http::Server::http((opts.host.as_str(), opts.port)).unwrap();

    if opts.slug {
        http_loop(&server, &slug::Server::new(&opts.config));
    } else {
        let balancer = ticket_sale_rocket::launch(&opts.config);

        thread::scope(|s| {
            for i in 0..opts.balancer_threads {
                thread::Builder::new()
                    .name(format!("balancer_{i}"))
                    .spawn_scoped(s, || http_loop(&server, &balancer))
                    .unwrap();
            }
        });
    }
}
