//! SSTableBuilder — 将严格递增的 InternalKey 序列写为单个 `.sst` 文件.
//!
//! # 构建流程
//!
//! ```text
//! add: 严格递增校验 → 写入 Data Block; 达到 block_size 即 flush (含 trailer + CRC)
//! finish: 收尾 Data Block → Bloom / Properties (raw) → Meta Index → Index → Footer
//!         → sync → rename .sst.tmp → .sst
//! abandon: 丢弃未完成的 .sst.tmp
//! ```
//!
//! # Invariant
//!
//! - 空 SST (0 entry) 的 `finish` 被拒绝 (`Error::InvalidArgument`).
//! - Data Block 达到 `block_size` 才 flush; Index / Meta Index 固定 `CompressionType::None`.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::config::CompressionType;
use crate::engine::filter::{BloomFilter, Filter};
use crate::engine::memtable::{compare_internal_key, extract_user_key};
use crate::error::{Error, Result};

use super::block::BlockBuilder;
use super::block_io::write_block;
use super::footer::Footer;
use super::handle::BlockHandle;
use super::index::{IndexBlockBuilder, IndexEntry};
use super::meta::{index_entry_for_bloom, write_raw_block};
use super::properties::SstProperties;

const MIN_BLOCK_SIZE: usize = 256;

pub struct SSTableBuilder {
    writer: BufWriter<File>,
    tmp_path: PathBuf,
    final_path: PathBuf,
    data_block_builder: BlockBuilder,
    index_block_builder: IndexBlockBuilder,
    last_key: Vec<u8>,
    data_block_offset: u64,
    num_entries: u64,
    raw_key_size: u64,
    raw_value_size: u64,
    block_size: usize,
    block_restart_interval: usize,
    compression: CompressionType,
    pending_handle: Option<BlockHandle>,
    block_count: u64,
    enable_bloom: bool,
    bloom_fp_rate: f64,
    bloom_filter: Option<BloomFilter>,
}

impl SSTableBuilder {
    pub fn new(
        path: &Path,
        block_size: usize,
        block_restart_interval: usize,
        compression: CompressionType,
        bloom_false_positive_rate: f64,
    ) -> Result<Self> {
        if block_size < MIN_BLOCK_SIZE {
            return Err(Error::InvalidArgument(format!(
                "block_size {block_size} < {MIN_BLOCK_SIZE}"
            )));
        }
        let tmp_path = path.with_extension("sst.tmp");
        if let Some(parent) = tmp_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(&tmp_path)?;
        let enable_bloom = bloom_false_positive_rate > 0.0;
        Ok(Self {
            writer: BufWriter::new(file),
            tmp_path,
            final_path: path.to_path_buf(),
            data_block_builder: BlockBuilder::new(block_restart_interval),
            index_block_builder: IndexBlockBuilder::new(),
            last_key: Vec::new(),
            data_block_offset: 0,
            num_entries: 0,
            raw_key_size: 0,
            raw_value_size: 0,
            block_size,
            block_restart_interval,
            compression,
            pending_handle: None,
            block_count: 0,
            enable_bloom,
            bloom_fp_rate: bloom_false_positive_rate,
            bloom_filter: None,
        })
    }

    pub fn set_expected_keys(&mut self, num_keys: usize) {
        if self.enable_bloom && self.bloom_filter.is_none() {
            self.bloom_filter = Some(BloomFilter::new(num_keys, self.bloom_fp_rate));
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidArgument("empty SSTable key".into()));
        }
        if !self.last_key.is_empty()
            && compare_internal_key(key, &self.last_key) != std::cmp::Ordering::Greater
        {
            return Err(Error::InvalidArgument(
                "SSTable keys must be strictly increasing".into(),
            ));
        }

        if let Some(handle) = self.pending_handle.take() {
            self.index_block_builder.add_entry(&IndexEntry {
                key: self.last_key.clone(),
                handle,
            })?;
        }

        self.data_block_builder.add(key, value)?;
        if self.data_block_builder.current_size() >= self.block_size {
            self.flush_data_block()?;
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);

        self.raw_key_size += key.len() as u64;
        self.raw_value_size += value.len() as u64;

        if self.enable_bloom {
            if self.bloom_filter.is_none() {
                // Safety net: callers should use set_expected_keys() for optimal size.
                // This fallback is only triggered if add() is called before set_expected_keys().
                self.bloom_filter = Some(BloomFilter::default_with_keys(128));
            }
            let user_key = extract_user_key(key);
            self.bloom_filter.as_mut().unwrap().add(user_key);
        }

        self.num_entries += 1;
        Ok(())
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block_builder.is_empty() {
            return Ok(());
        }
        let block_data = {
            let mut b = BlockBuilder::new(self.block_restart_interval);
            std::mem::swap(&mut self.data_block_builder, &mut b);
            b.finish()
        };
        let offset = self.data_block_offset;
        // `payload_len` 是压缩后 (或未压缩时原样) 实际落盘的字节数, **不能**用
        // `block_data.len()` (压缩前长度) 代替 —— 二者在启用压缩时不相等,
        // 用错会导致 handle.size / 后续 block offset 全部错位, 读侧拆 trailer
        // 时越界, 表现为 `Corruption("block CRC mismatch")` (回归 2026-07-02).
        let payload_len = write_block(&mut self.writer, &block_data, self.compression)?;
        let handle = BlockHandle {
            offset,
            size: payload_len + super::BLOCK_TRAILER_SIZE as u64,
        };
        self.data_block_offset += handle.size;
        self.pending_handle = Some(handle);
        self.block_count += 1;
        Ok(())
    }

    pub fn finish(mut self) -> Result<u64> {
        if self.num_entries == 0 {
            return Err(Error::InvalidArgument("empty sstable".into()));
        }

        if !self.data_block_builder.is_empty() {
            self.flush_data_block()?;
        }
        if let Some(handle) = self.pending_handle.take() {
            self.index_block_builder.add_entry(&IndexEntry {
                key: self.last_key.clone(),
                handle,
            })?;
        }

        let mut meta_index_builder = IndexBlockBuilder::new();
        if let Some(ref filter) = self.bloom_filter {
            let key_count = self.num_entries;
            let num_bits = filter.num_bits();
            let num_hashes = filter.num_hashes();
            let encoded = filter.encode();
            tracing::debug!(
              target: "sst",
              key_count,
              num_bits,
              num_hashes,
              encoded_len = encoded.len(),
              "bloom.build"
            );
            let bloom_handle =
                write_raw_block(&mut self.writer, &mut self.data_block_offset, &encoded)?;
            meta_index_builder.add_entry(&index_entry_for_bloom(bloom_handle))?;
        }

        // Properties Block
        {
            let props = SstProperties {
                num_entries: self.num_entries,
                raw_key_size: self.raw_key_size,
                raw_value_size: self.raw_value_size,
            };
            let encoded = props.encode();
            let props_handle =
                write_raw_block(&mut self.writer, &mut self.data_block_offset, &encoded)?;
            meta_index_builder.add_entry(&IndexEntry {
                key: super::meta::PROPERTIES_META_NAME.to_vec(),
                handle: props_handle,
            })?;
        }

        let meta_index_data = meta_index_builder.finish();
        let meta_index_offset = self.data_block_offset;
        let meta_index_payload_len = write_block(
            &mut self.writer,
            meta_index_data.as_ref(),
            CompressionType::None,
        )?;
        let meta_index_handle = BlockHandle {
            offset: meta_index_offset,
            size: meta_index_payload_len + super::BLOCK_TRAILER_SIZE as u64,
        };
        self.data_block_offset += meta_index_handle.size;

        let index_data = self.index_block_builder.finish();
        let index_offset = self.data_block_offset;
        let index_payload_len =
            write_block(&mut self.writer, index_data.as_ref(), CompressionType::None)?;
        let index_handle = BlockHandle {
            offset: index_offset,
            size: index_payload_len + super::BLOCK_TRAILER_SIZE as u64,
        };

        let footer = Footer::new(meta_index_handle, index_handle);
        self.writer.write_all(&footer.encode())?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        drop(self.writer);
        fs::rename(&self.tmp_path, &self.final_path)?;
        let file_size = fs::metadata(&self.final_path)?.len();

        tracing::debug!(
          target: "sst",
          block_count = self.block_count,
          file_size,
          bloom = self.bloom_filter.is_some(),
          "sst.build.complete"
        );
        Ok(file_size)
    }

    pub fn abandon(self) -> Result<()> {
        drop(self.writer);
        if self.tmp_path.exists() {
            fs::remove_file(&self.tmp_path)?;
        }
        Ok(())
    }

    pub fn block_count(&self) -> u64 {
        self.block_count
    }
}
