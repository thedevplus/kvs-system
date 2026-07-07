use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kvs::{KvStore, KvsEngine, Result, SledKvsEngine, error::KvError};
use rand::distr::{Alphanumeric, SampleString};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};

fn bench_write(c: &mut Criterion) -> Result<()> {
    let mut data = Vec::with_capacity(100usize);
    let kvs = KvStore::open("./benches-data")?;
    //let sled = SledKvsEngine::open("./benches-data")?;
    let mut key = String::new();
    let mut value = String::new();

    key.clear();
    value.clear();
    for i in 1..=100 {
        key = format!("Key{i}"); //Alphanumeric.sample_string(&mut rand::rng(), rand::random_range(1..=100000));
        value = "value".to_string(); //Alphanumeric.sample_string(&mut rand::rng(), rand::random_range(1..=100000));
        key.shrink_to_fit();
        value.shrink_to_fit();
        data.push((key, value));
    }
    let mut buffer = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open("./benches-data/data")?,
    );
    buffer.write_all(&serde_json::to_vec(&data)?)?;
    buffer.flush()?;

    let mut group = c.benchmark_group("write_group");
    group.bench_with_input(BenchmarkId::new("kvs", "set-rm-set"), &data, |b, d| {
        b.iter(|| {
            for e in d {
                assert!(kvs.set(e.0.clone(), e.1.clone()).is_ok());
            }
            for e in d {
                assert!(kvs.remove(e.0.clone()).is_ok());
            }
            for e in d {
                assert!(kvs.set(e.0.clone(), e.1.clone()).is_ok());
            }
        });
    });
    /*
    group.bench_with_input(BenchmarkId::new("sled", "set-rm-set"), &data, |b, d| {
        b.iter(|| {
            for e in d {
                assert!(sled.set(e.0.clone(), e.1.clone()).is_ok());
            }
            for e in d {
                assert!(sled.remove(e.0.clone()).is_ok());
            }
            for e in d {
                assert!(sled.set(e.0.clone(), e.1.clone()).is_ok());
            }
        });
    });
    */
    group.finish();
    Ok(())
}

fn bench_read(c: &mut Criterion) -> Result<()> {
    let kvs = KvStore::open("./benches-data")?;
    //let sled = SledKvsEngine::open("./benches-data")?;
    let file = BufReader::new(File::open("./benches-data/data")?);
    let data = serde_json::Deserializer::from_reader(file)
        .into_iter::<Vec<(String, String)>>()
        .map(|x| x)
        .next()
        .ok_or(KvError::File)??;
    let mut data_distr = Vec::with_capacity(100usize);
    for _ in 0..100 {
        data_distr.push(rand::random_range(0..100));
    }
    let arg = &(data_distr, data.clone());

    let mut group = c.benchmark_group("read_group");
    group.bench_with_input(BenchmarkId::new("kvs", "get-order"), &data, |b, d| {
        b.iter(|| {
            let mut count = 1u8;
            while count <= 10 {
                for e in d {
                    assert!(kvs.get(e.0.clone()).is_ok());
                }
                count += 1;
            }
        });
    });
    /*
    group.bench_with_input(BenchmarkId::new("sled", "get-order"), &data, |b, d| {
        b.iter(|| {
            let mut count = 1u8;
            while count <= 10 {
                for e in d {
                    assert!(sled.get(e.0.clone()).is_ok());
                }
                count += 1;
            }
        });
    });
    */
    group.bench_with_input(BenchmarkId::new("kvs", "get-disorder"), &arg, |b, d| {
        b.iter(|| {
            let mut count = 1u8;
            while count <= 10 {
                let index = &d.0;
                for e in index {
                    if let Ok(value) = kvs.get(d.1[*e].0.clone()) {
                        assert!(value.is_some());
                    } else {
                        eprintln!("Error occurs.");
                    }
                }
                count += 1;
            }
        });
    });
    /*
    group.bench_with_input(BenchmarkId::new("sled", "get-disorder"), &arg, |b, d| {
        b.iter(|| {
            let mut count = 1u8;
            while count <= 10 {
                let index = &d.0;
                for e in index {
                    if let Ok(value) = sled.get(d.1[*e].0.clone()) {
                        assert!(value.is_some());
                    } else {
                        eprintln!("Error occurs.");
                    }
                }
                count += 1;
            }
        });
    });
    */
    group.finish();
    Ok(())
}

fn group_benches(c: &mut Criterion) {
    if let Ok(_) = bench_write(c) {
        ()
    };
    if let Ok(_) = bench_read(c) {
        ()
    };
}

criterion_group!(benches, group_benches);
criterion_main!(benches);
