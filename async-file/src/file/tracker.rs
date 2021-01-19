use std::ops::RangeInclusive;
use std::sync::{Mutex, MutexGuard};

use crate::page_cache::Page;

/// A few tuning knobs for the sequential read tracker.
pub const MIN_PREFETCH_SIZE: usize = 4 * Page::size();
pub const MAX_PREFETCH_SIZE: usize = 64 * Page::size();
pub const MAX_CONCURRENCY: usize = 3;

/// A tracker for multiple concurrent sequential reads on a file.
///
/// If the tracker decides that a read is sequential, then it can help further decide
/// how much data to prefetch.
pub struct SeqRdTracker {
    trackers: [Mutex<Tracker>; MAX_CONCURRENCY],
}

// An internal tracker for a single thread of sequential reads.
struct Tracker {
    seq_window: RangeInclusive<usize>,
    prefetch_size: usize,
}

/// A sequential read.
pub struct SeqRd<'a> {
    tracker: MutexGuard<'a, Tracker>,
    offset: usize,
    len: usize,
}

// Implementation for SeqRdTracker

impl SeqRdTracker {
    pub fn new() -> Self {
        let trackers = array_init::array_init(|_| Mutex::new(Tracker::new()));
        Self { trackers }
    }

    /// Accept a new read.
    ///
    /// By accepting a new read, we track the read and guess---according to the
    /// previously accepted reads---whether the new read is sequential. If so,
    /// we return an object that represents the sequential read, which can in turn
    /// give a "good" suggestion for the amount of data to prefetch.
    pub fn accept(&self, offset: usize, len: usize) -> Option<SeqRd<'_>> {
        // Try to find a tracker of sequential reads that matches the new read.
        //
        // If not found, we pick a "victim" tracker to track the potentially new
        // thread of sequential reads starting from this read.
        let mut victim_tracker_opt: Option<MutexGuard<'_, Tracker>> = None;
        for (tracker_i, tracker_lock) in self.trackers.iter().enumerate() {
            let mut tracker = match tracker_lock.try_lock().ok() {
                Some(tracker) => tracker,
                None => continue,
            };

            if tracker.check_sequential(offset) {
                return Some(SeqRd::new(tracker, offset, len));
            } else {
                // Victim selection: we prefer the tracker with greater prefetch size.
                if let Some(victim_tracker) = victim_tracker_opt.as_mut() {
                    if victim_tracker.prefetch_size < tracker.prefetch_size {
                        victim_tracker_opt = Some(tracker);
                    }
                } else {
                    victim_tracker_opt = Some(tracker);
                }
            }
        }

        let mut victim_tracker = victim_tracker_opt?;
        victim_tracker.restart_from(offset, len);
        None
    }
}

// Implementation for Tracker

// The value of the prefetch size of a tracker that has not been able to track any
// sequential reads. This value is chosen so that our criterion of replacing a tracker
// can be simplified to "always choose the one with the greatest prefetch size".
const INVALID_PREFETCH_SIZE: usize = usize::max_value();

impl Tracker {
    pub fn new() -> Self {
        Self {
            seq_window: (0..=0),
            prefetch_size: INVALID_PREFETCH_SIZE,
        }
    }

    pub fn check_sequential(&self, offset: usize) -> bool {
        self.seq_window.contains(&offset)
    }

    pub fn restart_from(&mut self, offset: usize, len: usize) {
        self.seq_window = offset..=offset + len;
        self.prefetch_size = INVALID_PREFETCH_SIZE;
    }
}

// Implementation for SeqRd

impl<'a> SeqRd<'a> {
    fn new(mut tracker: MutexGuard<'a, Tracker>, offset: usize, len: usize) -> Self {
        if tracker.prefetch_size == INVALID_PREFETCH_SIZE {
            tracker.prefetch_size = MIN_PREFETCH_SIZE;
        }
        Self {
            tracker,
            offset,
            len,
        }
    }

    pub fn prefetch_size(&self) -> usize {
        self.tracker.prefetch_size
    }

    pub fn complete(mut self, read_bytes: usize) {
        debug_assert!(read_bytes > 0);
        self.tracker.seq_window = {
            let low = self.offset + read_bytes / 2;
            let upper = self.offset + read_bytes;
            low..=upper
        };

        self.tracker.prefetch_size *= 2;
        let max_prefetch_size = MAX_PREFETCH_SIZE.min(self.len * 2);
        if self.tracker.prefetch_size > max_prefetch_size {
            self.tracker.prefetch_size = max_prefetch_size;
        }
    }
}
