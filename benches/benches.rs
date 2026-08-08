use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kvs::protocol::{self, KvStream, StreamCommand};
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool};
use kvs::{
    KvCommand, KvStore, KvsClient, KvsServer, Result, SledKvsEngine, ThreadPool, error::KvError,
};
use rand::distr::{Alphanumeric, SampleString};
use std::fs::{DirBuilder, File, OpenOptions};
use std::hint::black_box;
use std::io::{BufReader, BufWriter, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Barrier, Weak};
use std::{process, thread};

const BENCH_LEN: usize = 10;
const BENCH_TOTAL: usize = 100;
const BENCH_PATH: &str = "./bench-data/";
const BENCH_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);

fn create_benchmark_data(path: impl Into<PathBuf>) -> Result<Vec<String>> {
    let mut path = path.into();
    let mut benchmark_data = Vec::with_capacity(BENCH_TOTAL);
    if !path.is_dir() {
        std::fs::DirBuilder::new().create(&path)?;
    }
    path.push("benchmark_data.txt");

    for _ in 1..=BENCH_TOTAL {
        let mut elem = Alphanumeric.sample_string(&mut rand::rng(), BENCH_LEN);
        elem.shrink_to_fit();
        benchmark_data.push(elem);
    }

    let mut buffer = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?,
    );
    buffer.write_all(&serde_json::to_vec(&benchmark_data)?)?;
    buffer.flush()?;

    Ok(benchmark_data)
}

fn get_benchmark_data(path: impl Into<PathBuf>) -> Result<Vec<String>> {
    let mut path = path.into();
    path.push("benchmark_data.txt");

    let buffer = BufReader::new(File::open(path)?);
    let benchmark_data = serde_json::Deserializer::from_reader(buffer)
        .into_iter::<Vec<String>>()
        .map(|x| x)
        .next()
        .ok_or(KvError::File)??;

    Ok(benchmark_data)
}

fn bench_kvs_shared_write<T: ThreadPool + Sync + Send>(
    c: &mut Criterion,
    client: Weak<KvsClient<T>>,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8.];
    let data = create_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");

    for e in cpu_num_ratio {
        if cpu_num % e == 0.0 {
            let cpu_num = (cpu_num / e) as u32;
            let mut path = path.clone();
            path.push(cpu_num.to_string());
            if !path.is_dir() {
                DirBuilder::new().create(&path)?;
            }
            let kvs = KvsServer::new(KvStore::open(&path)?, SharedQueueThreadPool::new(cpu_num)?)?;

            if let Ok(listener) = TcpListener::bind(addr) {
                let addr = listener.local_addr()?;
                let mut thread = SharedQueueThreadPool::new(1)?;
                thread.spawn(move || {
                    let _ = kvs.work(listener);
                });

                group.bench_with_input(
                    BenchmarkId::new("kvs-sharedqueue", cpu_num),
                    &(&addr, &data),
                    |b, d| {
                        let barrier = Arc::new(Barrier::new(BENCH_TOTAL));
                        let Some(client) = client.upgrade() else {
                            eprintln!("No client found");
                            process::exit(1);
                        };

                        b.iter(|| {
                            thread::scope(|_| {
                                d.1.iter().for_each(|e| {
                                    let barrier = Arc::downgrade(&barrier);

                                    assert!(
                                        client
                                            .run(
                                                black_box(KvCommand::Set),
                                                black_box(e.clone()),
                                                black_box(Some(e.clone())),
                                                black_box(*d.0),
                                                Some(barrier),
                                            )
                                            .is_ok()
                                    );
                                });
                            });
                        });
                    },
                );

                thread.join().unwrap();

                loop {
                    let Ok(mut tcp_stream) = TcpStream::connect(addr) else {
                        break;
                    };
                    tcp_stream.write_all(&protocol::create_protocol_stream(
                        &KvStream::build_from(
                            StreamCommand::Sd,
                            String::from("019fcd63-9284-70a5-8c5d-c3e0f1cbc573"),
                            None,
                        ),
                    )?)?;
                    tcp_stream.write_all(b"\n")?;
                }

                /*
                loop {
                    let mut assure_shutdown = 0u8;

                    let Ok(mut tcp_stream) = TcpStream::connect(addr) else {
                        if assure_shutdown > 1 {
                            break;
                        } else {
                            continue;
                        }
                    };

                    tcp_stream.write_all(&protocol::create_protocol_stream(
                        &KvStream::build_from(
                            StreamCommand::Sd,
                            String::from("019fcd63-9284-70a5-8c5d-c3e0f1cbc573"),
                            None,
                        ),
                    )?)?;
                    tcp_stream.write_all(b"\n")?;

                    assure_shutdown += 1;
                }

                thread.join().unwrap();
                */

                drop(thread);
                println!("Server thread drop success");
            }
        }
    }

    group.finish();
    Ok(())
}

fn bench_kvs_rayon_write<T: ThreadPool + Sync + Send>(
    c: &mut Criterion,
    client: Weak<KvsClient<T>>,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let data = create_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");

    for e in cpu_num_ratio {
        if cpu_num % e == 0.0 {
            let cpu_num = (cpu_num / e) as u32;
            let mut path = path.clone();
            path.push(cpu_num.to_string());
            if !path.is_dir() {
                DirBuilder::new().create(&path)?;
            }
            let kvs = KvsServer::new(KvStore::open(&path)?, RayonThreadPool::new(cpu_num)?)?;

            if let Ok(listener) = TcpListener::bind(addr) {
                let addr = listener.local_addr()?;
                rayon::spawn(move || {
                    let _ = kvs.work(listener);
                });

                group.bench_with_input(
                    BenchmarkId::new("kvs-rayon", cpu_num),
                    &(&addr, &data),
                    |b, d| {
                        let barrier = Arc::new(Barrier::new(BENCH_TOTAL));
                        let Some(client) = client.upgrade() else {
                            eprintln!("No client found");
                            process::exit(1);
                        };

                        b.iter(|| {
                            rayon::scope(|_| {
                                d.1.iter().for_each(|e| {
                                    let barrier = Arc::downgrade(&barrier);
                                    assert!(
                                        client
                                            .run(
                                                black_box(KvCommand::Set),
                                                black_box(e.clone()),
                                                black_box(Some(e.clone())),
                                                black_box(*d.0),
                                                Some(barrier),
                                            )
                                            .is_ok()
                                    );
                                });
                            });
                        });
                    },
                );

                loop {
                    let Ok(mut tcp_stream) = TcpStream::connect(addr) else {
                        break;
                    };
                    tcp_stream.write_all(&protocol::create_protocol_stream(
                        &KvStream::build_from(
                            StreamCommand::Sd,
                            String::from("019fcd63-9284-70a5-8c5d-c3e0f1cbc573"),
                            None,
                        ),
                    )?)?;
                    tcp_stream.write_all(b"\n")?;
                }
            }
        }
    }

    group.finish();
    Ok(())
}

fn bench_sled_shared_write<T: ThreadPool + Sync + Send>(
    c: &mut Criterion,
    client: Weak<KvsClient<T>>,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let data = create_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");

    for e in cpu_num_ratio {
        if cpu_num % e == 0.0 {
            let cpu_num = (cpu_num / e) as u32;
            let mut path = path.clone();
            path.push(cpu_num.to_string());
            if !path.is_dir() {
                DirBuilder::new().create(&path)?;
            }
            let kvs = KvsServer::new(
                SledKvsEngine::open(&path)?,
                SharedQueueThreadPool::new(cpu_num)?,
            )?;

            if let Ok(listener) = TcpListener::bind(addr) {
                let addr = listener.local_addr()?;
                let thread = thread::spawn(move || {
                    let _ = kvs.work(listener);
                });

                group.bench_with_input(
                    BenchmarkId::new("sled-sharedqueue", cpu_num),
                    &(&addr, &data),
                    |b, d| {
                        let barrier = Arc::new(Barrier::new(BENCH_TOTAL));
                        let Some(client) = client.upgrade() else {
                            eprintln!("No client found");
                            process::exit(1);
                        };

                        b.iter(|| {
                            thread::scope(|_| {
                                d.1.iter().for_each(|e| {
                                    let barrier = Arc::downgrade(&barrier);
                                    assert!(
                                        client
                                            .run(
                                                black_box(KvCommand::Set),
                                                black_box(e.clone()),
                                                black_box(Some(e.clone())),
                                                black_box(*d.0),
                                                Some(barrier),
                                            )
                                            .is_ok()
                                    );
                                });
                            });
                        });
                    },
                );

                loop {
                    let Ok(mut tcp_stream) = TcpStream::connect(addr) else {
                        break;
                    };
                    tcp_stream.write_all(&protocol::create_protocol_stream(
                        &KvStream::build_from(
                            StreamCommand::Sd,
                            String::from("019fcd63-9284-70a5-8c5d-c3e0f1cbc573"),
                            None,
                        ),
                    )?)?;
                    tcp_stream.write_all(b"\n")?;
                }

                thread.join().unwrap();
            }
        }
    }

    group.finish();
    Ok(())
}

fn bench_sled_rayon_write<T: ThreadPool + Sync + Send>(
    c: &mut Criterion,
    client: Weak<KvsClient<T>>,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let data = create_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");

    for e in cpu_num_ratio {
        if cpu_num % e == 0.0 {
            let cpu_num = (cpu_num / e) as u32;
            let mut path = path.clone();
            path.push(cpu_num.to_string());
            if !path.is_dir() {
                DirBuilder::new().create(&path)?;
            }
            let kvs = KvsServer::new(SledKvsEngine::open(&path)?, RayonThreadPool::new(cpu_num)?)?;

            if let Ok(listener) = TcpListener::bind(addr) {
                let addr = listener.local_addr()?;
                rayon::spawn(move || {
                    let _ = kvs.work(listener);
                });

                group.bench_with_input(
                    BenchmarkId::new("sled-rayon", cpu_num),
                    &(&addr, &data),
                    |b, d| {
                        let barrier = Arc::new(Barrier::new(BENCH_TOTAL));
                        let Some(client) = client.upgrade() else {
                            eprintln!("No client found");
                            process::exit(1);
                        };

                        b.iter(|| {
                            rayon::scope(|_| {
                                d.1.iter().for_each(|e| {
                                    let barrier = Arc::downgrade(&barrier);
                                    assert!(
                                        client
                                            .run(
                                                black_box(KvCommand::Set),
                                                black_box(e.clone()),
                                                black_box(Some(e.clone())),
                                                black_box(*d.0),
                                                Some(barrier),
                                            )
                                            .is_ok()
                                    );
                                });
                            });
                        });
                    },
                );

                loop {
                    let Ok(mut tcp_stream) = TcpStream::connect(addr) else {
                        break;
                    };
                    tcp_stream.write_all(&protocol::create_protocol_stream(
                        &KvStream::build_from(
                            StreamCommand::Sd,
                            String::from("019fcd63-9284-70a5-8c5d-c3e0f1cbc573"),
                            None,
                        ),
                    )?)?;
                    tcp_stream.write_all(b"\n")?;
                }
            }
        }
    }

    group.finish();
    Ok(())
}

fn bench_kvs_shared_read<T: ThreadPool + Sync + Send>(
    c: &mut Criterion,
    client: Weak<KvsClient<T>>,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let data = create_benchmark_data(&path)?;
    let mut group = c.benchmark_group("read_group");

    for e in cpu_num_ratio {
        if cpu_num % e == 0.0 {
            let cpu_num = (cpu_num / e) as u32;
            let mut path = path.clone();
            path.push(cpu_num.to_string());
            if !path.is_dir() {
                DirBuilder::new().create(&path)?;
            }
            let kvs = KvsServer::new(KvStore::open(&path)?, SharedQueueThreadPool::new(cpu_num)?)?;

            if let Ok(listener) = TcpListener::bind(addr) {
                let addr = listener.local_addr()?;
                let thread = thread::spawn(move || {
                    let _ = kvs.work(listener);
                });

                group.bench_with_input(
                    BenchmarkId::new("kvs-sharedqueue", cpu_num),
                    &(&addr, &data),
                    |b, d| {
                        let barrier = Arc::new(Barrier::new(BENCH_TOTAL));
                        let Some(client) = client.upgrade() else {
                            eprintln!("No client found");
                            process::exit(1);
                        };

                        b.iter(|| {
                            thread::scope(|_| {
                                d.1.iter().for_each(|e| {
                                    let barrier = Arc::downgrade(&barrier);
                                    assert!(
                                        client
                                            .run(
                                                black_box(KvCommand::Get),
                                                black_box(e.clone()),
                                                black_box(None),
                                                black_box(*d.0),
                                                Some(barrier),
                                            )
                                            .is_ok()
                                    );
                                });
                            });
                        });
                    },
                );

                loop {
                    let mut assure_shutdown = 0u8;

                    let Ok(mut tcp_stream) = TcpStream::connect(addr) else {
                        if assure_shutdown > 1 {
                            break;
                        } else {
                            continue;
                        }
                    };

                    tcp_stream.write_all(&protocol::create_protocol_stream(
                        &KvStream::build_from(
                            StreamCommand::Sd,
                            String::from("019fcd63-9284-70a5-8c5d-c3e0f1cbc573"),
                            None,
                        ),
                    )?)?;
                    tcp_stream.write_all(b"\n")?;

                    assure_shutdown += 1;
                }

                thread.join().unwrap();
                println!("Server thread drop success");
            }
        }
    }

    group.finish();
    Ok(())
}

fn bench_kvs_rayon_read<T: ThreadPool + Sync + Send>(
    c: &mut Criterion,
    client: Weak<KvsClient<T>>,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let data = create_benchmark_data(&path)?;
    let mut group = c.benchmark_group("read_group");

    for e in cpu_num_ratio {
        if cpu_num % e == 0.0 {
            let cpu_num = (cpu_num / e) as u32;
            let mut path = path.clone();
            path.push(cpu_num.to_string());
            if !path.is_dir() {
                DirBuilder::new().create(&path)?;
            }
            let kvs = KvsServer::new(KvStore::open(&path)?, RayonThreadPool::new(cpu_num)?)?;

            if let Ok(listener) = TcpListener::bind(addr) {
                let addr = listener.local_addr()?;
                rayon::spawn(move || {
                    let _ = kvs.work(listener);
                });

                group.bench_with_input(
                    BenchmarkId::new("kvs-rayon", cpu_num),
                    &(&addr, &data),
                    |b, d| {
                        let barrier = Arc::new(Barrier::new(BENCH_TOTAL));
                        let Some(client) = client.upgrade() else {
                            eprintln!("No client found");
                            process::exit(1);
                        };

                        b.iter(|| {
                            rayon::scope(|_| {
                                d.1.iter().for_each(|e| {
                                    let barrier = Arc::downgrade(&barrier);
                                    assert!(
                                        client
                                            .run(
                                                black_box(KvCommand::Get),
                                                black_box(e.clone()),
                                                black_box(None),
                                                black_box(*d.0),
                                                Some(barrier),
                                            )
                                            .is_ok()
                                    );
                                });
                            });
                        });
                    },
                );

                loop {
                    let Ok(mut tcp_stream) = TcpStream::connect(addr) else {
                        break;
                    };
                    tcp_stream.write_all(&protocol::create_protocol_stream(
                        &KvStream::build_from(
                            StreamCommand::Sd,
                            String::from("019fcd63-9284-70a5-8c5d-c3e0f1cbc573"),
                            None,
                        ),
                    )?)?;
                    tcp_stream.write_all(b"\n")?;
                }
            }
        }
    }

    group.finish();
    Ok(())
}

fn bench_sled_shared_read<T: ThreadPool + Sync + Send>(
    c: &mut Criterion,
    client: Weak<KvsClient<T>>,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let data = create_benchmark_data(&path)?;
    let mut group = c.benchmark_group("read_group");

    for e in cpu_num_ratio {
        if cpu_num % e == 0.0 {
            let cpu_num = (cpu_num / e) as u32;
            let mut path = path.clone();
            path.push(cpu_num.to_string());
            if !path.is_dir() {
                DirBuilder::new().create(&path)?;
            }
            let kvs = KvsServer::new(
                SledKvsEngine::open(&path)?,
                SharedQueueThreadPool::new(cpu_num)?,
            )?;

            if let Ok(listener) = TcpListener::bind(addr) {
                let addr = listener.local_addr()?;
                let thread = thread::spawn(move || {
                    let _ = kvs.work(listener);
                });

                group.bench_with_input(
                    BenchmarkId::new("sled-sharedqueue", cpu_num),
                    &(&addr, &data),
                    |b, d| {
                        let barrier = Arc::new(Barrier::new(BENCH_TOTAL));
                        let Some(client) = client.upgrade() else {
                            eprintln!("No client found");
                            process::exit(1);
                        };

                        b.iter(|| {
                            thread::scope(|_| {
                                d.1.iter().for_each(|e| {
                                    let barrier = Arc::downgrade(&barrier);
                                    assert!(
                                        client
                                            .run(
                                                black_box(KvCommand::Get),
                                                black_box(e.clone()),
                                                black_box(None),
                                                black_box(*d.0),
                                                Some(barrier),
                                            )
                                            .is_ok()
                                    );
                                });
                            });
                        });
                    },
                );

                loop {
                    let Ok(mut tcp_stream) = TcpStream::connect(addr) else {
                        break;
                    };
                    tcp_stream.write_all(&protocol::create_protocol_stream(
                        &KvStream::build_from(
                            StreamCommand::Sd,
                            String::from("019fcd63-9284-70a5-8c5d-c3e0f1cbc573"),
                            None,
                        ),
                    )?)?;
                    tcp_stream.write_all(b"\n")?;
                }

                thread.join().unwrap();
            }
        }
    }

    group.finish();
    Ok(())
}

fn bench_sled_rayon_read<T: ThreadPool + Sync + Send>(
    c: &mut Criterion,
    client: Weak<KvsClient<T>>,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let data = create_benchmark_data(&path)?;
    let mut group = c.benchmark_group("read_group");

    for e in cpu_num_ratio {
        if cpu_num % e == 0.0 {
            let cpu_num = (cpu_num / e) as u32;
            let mut path = path.clone();
            path.push(cpu_num.to_string());
            if !path.is_dir() {
                DirBuilder::new().create(&path)?;
            }
            let kvs = KvsServer::new(SledKvsEngine::open(&path)?, RayonThreadPool::new(cpu_num)?)?;

            if let Ok(listener) = TcpListener::bind(addr) {
                let addr = listener.local_addr()?;
                rayon::spawn(move || {
                    let _ = kvs.work(listener);
                });

                group.bench_with_input(
                    BenchmarkId::new("sled-rayon", cpu_num),
                    &(&addr, &data),
                    |b, d| {
                        let barrier = Arc::new(Barrier::new(BENCH_TOTAL));
                        let Some(client) = client.upgrade() else {
                            eprintln!("No client found");
                            process::exit(1);
                        };

                        b.iter(|| {
                            rayon::scope(|_| {
                                d.1.iter().for_each(|e| {
                                    let barrier = Arc::downgrade(&barrier);
                                    assert!(
                                        client
                                            .run(
                                                black_box(KvCommand::Get),
                                                black_box(e.clone()),
                                                black_box(None),
                                                black_box(*d.0),
                                                Some(barrier),
                                            )
                                            .is_ok()
                                    );
                                });
                            });
                        });
                    },
                );

                loop {
                    let Ok(mut tcp_stream) = TcpStream::connect(addr) else {
                        break;
                    };
                    tcp_stream.write_all(&protocol::create_protocol_stream(
                        &KvStream::build_from(
                            StreamCommand::Sd,
                            String::from("019fcd63-9284-70a5-8c5d-c3e0f1cbc573"),
                            None,
                        ),
                    )?)?;
                    tcp_stream.write_all(b"\n")?;
                }
            }
        }
    }

    group.finish();
    Ok(())
}

fn group_benches(c: &mut Criterion) {
    let Ok(shared_thread) = SharedQueueThreadPool::new(BENCH_TOTAL as u32) else {
        return;
    };
    let Ok(rayon_thread) = RayonThreadPool::new(BENCH_TOTAL as u32) else {
        return;
    };
    let Ok(client_shared) = KvsClient::new(shared_thread) else {
        return;
    };
    let Ok(client_rayon) = KvsClient::new(rayon_thread) else {
        return;
    };

    let client_shared = Arc::new(client_shared);
    let client_rayon = Arc::new(client_rayon);

    if let Err(e) =
        bench_kvs_shared_write(c, Arc::downgrade(&client_shared), &BENCH_PATH, BENCH_ADDR)
    {
        eprintln!("Benchmark error: {e}");
    }

    drop(client_shared);
    drop(client_rayon);

    println!("Benchmark finiiiiish");
}

criterion_group!(benches, group_benches);
criterion_main!(benches);
