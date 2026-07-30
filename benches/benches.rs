use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool};
use kvs::{
    KvCommand, KvStore, KvsClient, KvsServer, Result, SledKvsEngine, ThreadPool, error::KvError,
};
use rand::distr::{Alphanumeric, SampleString};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;

const BENCH_LEN: usize = 1000;
const BENCH_TOTAL: usize = 1000;
const BENCH_PATH: &str = "./bench-data/";
const BENCH_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000);

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

fn bench_kvs_shared_write(
    c: &mut Criterion,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let data = create_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");

    for e in cpu_num_ratio {
        group.bench_with_input(
            BenchmarkId::new("kvs-shared-set-rm-set", e),
            &(&path, &addr, &data),
            |b, d| {
                if cpu_num % e == 0.0 {
                    let cpu_num = (cpu_num / e) as u32;
                    let thread_pool_server = SharedQueueThreadPool::new(cpu_num).unwrap();
                    let thread_pool_client =
                        SharedQueueThreadPool::new(BENCH_TOTAL as u32).unwrap();
                    let kvs =
                        KvsServer::new(KvStore::open(d.0).unwrap(), thread_pool_server).unwrap();
                    let client = KvsClient::new(thread_pool_client).unwrap();

                    b.iter(|| {
                        let listener = TcpListener::bind(d.1).unwrap();
                        assert!(kvs.work(listener).is_ok());
                    });

                    d.2.iter().for_each(|e| {
                        assert!(
                            client
                                .run(KvCommand::Set, e.clone(), Some(e.clone()), *d.1)
                                .is_ok()
                        );
                    });
                    d.2.iter().for_each(|e| {
                        assert!(client.run(KvCommand::Rm, e.clone(), None, *d.1).is_ok());
                    });
                    d.2.iter().for_each(|e| {
                        assert!(
                            client
                                .run(KvCommand::Set, e.clone(), Some(e.clone()), *d.1)
                                .is_ok()
                        );
                    });
                }
            },
        );
    }

    group.finish();
    Ok(())
}

fn bench_sled_shared_write(
    c: &mut Criterion,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let data = get_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");

    for e in cpu_num_ratio {
        group.bench_with_input(
            BenchmarkId::new("sled-shared-set-rm-set", e),
            &(&path, &addr, &data),
            |b, d| {
                if cpu_num % e == 0.0 {
                    let cpu_num = (cpu_num / e) as u32;
                    let thread_pool_server = SharedQueueThreadPool::new(cpu_num).unwrap();
                    let thread_pool_client =
                        SharedQueueThreadPool::new(BENCH_TOTAL as u32).unwrap();
                    let kvs = KvsServer::new(SledKvsEngine::open(d.0).unwrap(), thread_pool_server)
                        .unwrap();
                    let client = KvsClient::new(thread_pool_client).unwrap();

                    b.iter(|| {
                        let listener = TcpListener::bind(d.1).unwrap();
                        assert!(kvs.work(listener).is_ok());
                    });

                    d.2.iter().for_each(|e| {
                        assert!(
                            client
                                .run(KvCommand::Set, e.clone(), Some(e.clone()), *d.1)
                                .is_ok()
                        );
                    });
                    d.2.iter().for_each(|e| {
                        assert!(client.run(KvCommand::Rm, e.clone(), None, *d.1).is_ok());
                    });
                    d.2.iter().for_each(|e| {
                        assert!(
                            client
                                .run(KvCommand::Set, e.clone(), Some(e.clone()), *d.1)
                                .is_ok()
                        );
                    });
                }
            },
        );
    }

    group.finish();
    Ok(())
}

fn bench_kvs_rayon_write(
    c: &mut Criterion,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let data = get_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");

    for e in cpu_num_ratio {
        group.bench_with_input(
            BenchmarkId::new("kvs-rayon-set-rm-set", e),
            &(&path, &addr, &data),
            |b, d| {
                if cpu_num % e == 0.0 {
                    let cpu_num = (cpu_num / e) as u32;
                    let thread_pool_server = RayonThreadPool::new(cpu_num).unwrap();
                    let thread_pool_client = RayonThreadPool::new(BENCH_TOTAL as u32).unwrap();
                    let kvs =
                        KvsServer::new(KvStore::open(d.0).unwrap(), thread_pool_server).unwrap();
                    let client = KvsClient::new(thread_pool_client).unwrap();

                    b.iter(|| {
                        let listener = TcpListener::bind(d.1).unwrap();
                        assert!(kvs.work(listener).is_ok());
                    });

                    d.2.iter().for_each(|e| {
                        assert!(
                            client
                                .run(KvCommand::Set, e.clone(), Some(e.clone()), *d.1)
                                .is_ok()
                        );
                    });
                    d.2.iter().for_each(|e| {
                        assert!(client.run(KvCommand::Rm, e.clone(), None, *d.1).is_ok());
                    });
                    d.2.iter().for_each(|e| {
                        assert!(
                            client
                                .run(KvCommand::Set, e.clone(), Some(e.clone()), *d.1)
                                .is_ok()
                        );
                    });
                }
            },
        );
    }

    group.finish();
    Ok(())
}

fn bench_sled_rayon_write(
    c: &mut Criterion,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let data = get_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");

    for e in cpu_num_ratio {
        group.bench_with_input(
            BenchmarkId::new("sled-rayon-set-rm-set", e),
            &(&path, &addr, &data),
            |b, d| {
                if cpu_num % e == 0.0 {
                    let cpu_num = (cpu_num / e) as u32;
                    let thread_pool_server = RayonThreadPool::new(cpu_num).unwrap();
                    let thread_pool_client = RayonThreadPool::new(BENCH_TOTAL as u32).unwrap();
                    let kvs = KvsServer::new(SledKvsEngine::open(d.0).unwrap(), thread_pool_server)
                        .unwrap();
                    let client = KvsClient::new(thread_pool_client).unwrap();

                    b.iter(|| {
                        let listener = TcpListener::bind(d.1).unwrap();
                        assert!(kvs.work(listener).is_ok());
                    });

                    d.2.iter().for_each(|e| {
                        assert!(
                            client
                                .run(KvCommand::Set, e.clone(), Some(e.clone()), *d.1)
                                .is_ok()
                        );
                    });
                    d.2.iter().for_each(|e| {
                        assert!(client.run(KvCommand::Rm, e.clone(), None, *d.1).is_ok());
                    });
                    d.2.iter().for_each(|e| {
                        assert!(
                            client
                                .run(KvCommand::Set, e.clone(), Some(e.clone()), *d.1)
                                .is_ok()
                        );
                    });
                }
            },
        );
    }

    group.finish();
    Ok(())
}

fn bench_kvs_shared_read(
    c: &mut Criterion,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let mut data = get_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");
    data.sort(); // Not read in write log order

    for e in cpu_num_ratio {
        group.bench_with_input(
            BenchmarkId::new("kvs-shared-get-dequeue", e),
            &(&path, &addr, &data),
            |b, d| {
                if cpu_num % e == 0.0 {
                    let cpu_num = (cpu_num / e) as u32;
                    let thread_pool_server = SharedQueueThreadPool::new(cpu_num).unwrap();
                    let thread_pool_client =
                        SharedQueueThreadPool::new(BENCH_TOTAL as u32).unwrap();
                    let kvs =
                        KvsServer::new(KvStore::open(d.0).unwrap(), thread_pool_server).unwrap();
                    let client = KvsClient::new(thread_pool_client).unwrap();

                    b.iter(|| {
                        let listener = TcpListener::bind(d.1).unwrap();
                        assert!(kvs.work(listener).is_ok());
                    });

                    d.2.iter().for_each(|e| {
                        assert!(client.run(KvCommand::Get, e.clone(), None, *d.1).is_ok());
                    });
                }
            },
        );
    }

    group.finish();
    Ok(())
}

fn bench_sled_shared_read(
    c: &mut Criterion,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let mut data = get_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");
    data.sort(); // Not read in write log order

    for e in cpu_num_ratio {
        group.bench_with_input(
            BenchmarkId::new("sled-shared-get-dequeue", e),
            &(&path, &addr, &data),
            |b, d| {
                if cpu_num % e == 0.0 {
                    let cpu_num = (cpu_num / e) as u32;
                    let thread_pool_server = SharedQueueThreadPool::new(cpu_num).unwrap();
                    let thread_pool_client =
                        SharedQueueThreadPool::new(BENCH_TOTAL as u32).unwrap();
                    let kvs = KvsServer::new(SledKvsEngine::open(d.0).unwrap(), thread_pool_server)
                        .unwrap();
                    let client = KvsClient::new(thread_pool_client).unwrap();

                    b.iter(|| {
                        let listener = TcpListener::bind(d.1).unwrap();
                        assert!(kvs.work(listener).is_ok());
                    });

                    d.2.iter().for_each(|e| {
                        assert!(client.run(KvCommand::Get, e.clone(), None, *d.1).is_ok());
                    });
                }
            },
        );
    }

    group.finish();
    Ok(())
}

fn bench_kvs_rayon_read(
    c: &mut Criterion,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let mut data = get_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");
    data.sort(); // Not read in write log order

    for e in cpu_num_ratio {
        group.bench_with_input(
            BenchmarkId::new("kvs-rayon-get-dequeue", e),
            &(&path, &addr, &data),
            |b, d| {
                if cpu_num % e == 0.0 {
                    let cpu_num = (cpu_num / e) as u32;
                    let thread_pool_server = RayonThreadPool::new(cpu_num).unwrap();
                    let thread_pool_client = RayonThreadPool::new(BENCH_TOTAL as u32).unwrap();
                    let kvs =
                        KvsServer::new(KvStore::open(d.0).unwrap(), thread_pool_server).unwrap();
                    let client = KvsClient::new(thread_pool_client).unwrap();

                    b.iter(|| {
                        let listener = TcpListener::bind(d.1).unwrap();
                        assert!(kvs.work(listener).is_ok());
                    });

                    d.2.iter().for_each(|e| {
                        assert!(client.run(KvCommand::Get, e.clone(), None, *d.1).is_ok());
                    });
                }
            },
        );
    }

    group.finish();
    Ok(())
}

fn bench_sled_rayon_read(
    c: &mut Criterion,
    path: impl Into<PathBuf>,
    addr: SocketAddr,
) -> Result<()> {
    let path = path.into();
    let cpu_num = num_cpus::get() as f32;
    let cpu_num_ratio: Vec<f32> = vec![1. / 8., 1. / 4., 1. / 2., 1., 2., 4., 8.];
    let mut data = get_benchmark_data(&path)?;
    let mut group = c.benchmark_group("write_group");
    data.sort(); // Not read in write log order

    for e in cpu_num_ratio {
        group.bench_with_input(
            BenchmarkId::new("sled-rayon-get-dequeue", e),
            &(&path, &addr, &data),
            |b, d| {
                if cpu_num % e == 0.0 {
                    let cpu_num = (cpu_num / e) as u32;
                    let thread_pool_server = RayonThreadPool::new(cpu_num).unwrap();
                    let thread_pool_client = RayonThreadPool::new(BENCH_TOTAL as u32).unwrap();
                    let kvs = KvsServer::new(SledKvsEngine::open(d.0).unwrap(), thread_pool_server)
                        .unwrap();
                    let client = KvsClient::new(thread_pool_client).unwrap();

                    b.iter(|| {
                        let listener = TcpListener::bind(d.1).unwrap();
                        assert!(kvs.work(listener).is_ok());
                    });

                    d.2.iter().for_each(|e| {
                        assert!(client.run(KvCommand::Get, e.clone(), None, *d.1).is_ok());
                    });
                }
            },
        );
    }

    group.finish();
    Ok(())
}

fn group_benches(c: &mut Criterion) {
    let _ = bench_kvs_shared_write(c, &BENCH_PATH, BENCH_ADDR);
    let _ = bench_sled_shared_write(c, &BENCH_PATH, BENCH_ADDR);
    let _ = bench_kvs_rayon_write(c, &BENCH_PATH, BENCH_ADDR);
    let _ = bench_sled_rayon_write(c, &BENCH_PATH, BENCH_ADDR);

    let _ = bench_kvs_shared_read(c, &BENCH_PATH, BENCH_ADDR);
    let _ = bench_sled_shared_read(c, &BENCH_PATH, BENCH_ADDR);
    let _ = bench_kvs_rayon_read(c, &BENCH_PATH, BENCH_ADDR);
    let _ = bench_sled_rayon_read(c, &BENCH_PATH, BENCH_ADDR);
}

criterion_group!(benches, group_benches);
criterion_main!(benches);
