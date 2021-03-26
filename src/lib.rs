use bloomfilter::Bloom;
use bincode;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::time::{Instant, Duration};
use log::info;

const CHECKPOINT_SECS: u64 = 300;

#[derive(Serialize, Deserialize)]
struct SerializedFilter {
    bitmap: Vec<u8>,
    k_num: u32,
    sip_keys: [(u64, u64); 2],
}

pub struct Filter {
    bloom: Bloom<u64>,
    checkpoint_path: PathBuf,
    log_file: File,
    log_path: PathBuf,
    prev_checkpoint: Instant,
}

fn save_checkpoint(bloom: &Bloom<u64>, path: &Path) {
    let serialized = SerializedFilter {
        bitmap: bloom.bitmap().to_vec(),
        k_num: bloom.number_of_hash_functions(),
        sip_keys: bloom.sip_keys(),
    };
    let temp_path = path.with_extension("temp");
    // let mut file = File::create(&temp_path).unwrap();
    info!("serializing bloom filter...");
    let bytes = bincode::serialize(&serialized).unwrap();
    info!("writing to file...");
    std::fs::write(&temp_path, &bytes).unwrap();
    info!("done!");
    // bincode::serialize_into(&mut file, &serialized).unwrap();
    std::fs::rename(temp_path, path).unwrap();
}

impl Filter {
    pub fn new(
        dir: PathBuf,
        n_bytes: usize,
        expected_entries: usize,
    ) -> Filter {
        let checkpoint_path = dir.join("checkpoint.bincode");
        let log_path = dir.join("wal.log");
        let bloom = if dir.exists() {
            info!("loading bloom filter from checkpoint...");
            let checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();
            let checkpoint: SerializedFilter = bincode::deserialize(
                &checkpoint_bytes).unwrap();
            let mut bloom = Bloom::from_existing(
                &checkpoint.bitmap,
                checkpoint.bitmap.len() as u64,
                checkpoint.k_num,
                checkpoint.sip_keys,
            );
            let log_file = File::open(&log_path).unwrap();
            for url in BufReader::new(log_file).lines() {
                bloom.set(&url.unwrap().parse::<u64>().unwrap());
            }
            info!("done");
            bloom
        } else {
            std::fs::create_dir_all(dir).unwrap();
            Bloom::new(n_bytes, expected_entries)
        };
        save_checkpoint(&bloom, &checkpoint_path);
        // truncate log file
        let log_file = File::create(&log_path).unwrap();
        Filter {
            bloom,
            checkpoint_path,
            log_file,
            log_path,
            prev_checkpoint: Instant::now(),
        }
    }

    pub fn set(&mut self, url: u64) {
        self.bloom.set(&url);
        writeln!(self.log_file, "{}", url).unwrap();
        if Instant::now() > self.prev_checkpoint + Duration::from_secs(CHECKPOINT_SECS) {
            info!("checkpointing bloom filter");
            self.prev_checkpoint = Instant::now();
            save_checkpoint(&self.bloom, &self.checkpoint_path);
            // truncate log file
            self.log_file = File::create(&self.log_path).unwrap();
        }
    }

    pub fn check(&mut self, url: u64) -> bool {
        self.bloom.check(&url)
    }
}
