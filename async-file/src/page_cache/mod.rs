use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

mod page;
mod page_entry;
mod page_handle;
mod page_lru_list;
mod page_state;

pub use self::page::Page;
pub use self::page_handle::PageHandle;
pub use self::page_state::PageState;

use self::page_entry::{PageEntry, PageEntryInner};
use self::page_lru_list::PageLruList;

/// Page cache.
pub struct PageCache {
    capacity: usize,
    num_allocated: AtomicUsize,
    map: Mutex<HashMap<(i32, usize), PageEntry>>,
    lru_lists: [Mutex<PageLruList>; 3],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum LruListName {
    // For any entry : &PageEntry in &lru_lists[LruListName::Unused], we have
    //      entry.state == PageState::Uninit && PageEntry::refcnt(entry) == 1
    Unused = 0,
    // For any entry : &PageEntry in &lru_lists[LruListName::Evictable], we have
    //      entry.state == PageState::UpToDate && PageEntry::refcnt(entry) == 2
    Evictable = 1,
    // For any entry : &PageEntry in &lru_lists[LruListName::Dirty], we have
    //      entry.state == PageState::Dirty (most likely, but not always)
    //   && PageEntry::refcnt(entry) > 2
    Dirty = 2,
}

impl PageCache {
    /// Create a page cache that can contain an specified number of pages at most.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0);
        let num_allocated = AtomicUsize::new(capacity);
        let map = Mutex::new(HashMap::new());
        let lru_lists = array_init::array_init(|_| Mutex::new(PageLruList::new()));
        Self {
            capacity,
            num_allocated,
            map,
            lru_lists,
        }
    }

    /// Acquire a page handle for the given fd and offset.
    ///
    /// The returned page handle may be fetched from the cache or newly created.
    pub fn acquire(&self, fd: i32, offset: usize) -> Option<PageHandle> {
        let key = (fd, offset);
        let mut map = self.map.lock().unwrap();

        // Try to get an existing entry in the map.
        if let Some(existing_entry) = map.get(&key) {
            self.touch_lru_list(existing_entry);
            return Some(PageHandle::wrap(existing_entry.clone()));
        }

        // Try to create an new entry
        // First attempt: reuse an entry that is previously allocated, but currently not in use.
        let reusable_entry_opt = self.evict_from_lru_list(LruListName::Unused);
        let new_entry = if let Some(mut reusable_entry) = reusable_entry_opt {
            unsafe {
                debug_assert!(PageEntry::refcnt(&reusable_entry) == 1);
                reusable_entry.reset_key((fd, offset));
            }
            reusable_entry
        }
        // Second attempt: allocate a new entry if the capacity won't be exceeded
        else if self.num_allocated.load(Ordering::Relaxed) < self.capacity {
            self.num_allocated.fetch_add(1, Ordering::Relaxed);
            PageEntry::new(fd, offset)
        }
        // Last attempt: evict an entry from the evictable LRU list
        else {
            let evicted_entry_opt = self.evict_from_lru_list(LruListName::Evictable);
            let mut evicted_entry = match evicted_entry_opt {
                Some(evicted_entry) => evicted_entry,
                None => {
                    return None;
                }
            };
            map.remove(&evicted_entry.key());

            unsafe {
                debug_assert!(PageEntry::refcnt(&evicted_entry) == 1);
                evicted_entry.reset_key((fd, offset));
                *evicted_entry.state() = PageState::Uninit;
            }
            evicted_entry
        };
        map.insert(key, new_entry.clone()).unwrap_none();

        Some(PageHandle::wrap(new_entry))
    }

    /// Release a page handle.
    pub fn release(&self, handle: PageHandle) {
        self.do_release(handle, false)
    }

    pub fn discard(&self, handle: PageHandle) {
        self.do_release(handle, true)
    }

    /// Evict some LRU dirty pages.
    ///
    /// Note that the results may contain false positives.
    pub fn evict_dirty_pages(&self, max_count: usize) -> Vec<PageHandle> {
        let mut lru_dirty_list = self.acquire_lru_list(LruListName::Dirty);
        let evicted: Vec<PageEntry> = lru_dirty_list.evict_nr(max_count);
        unsafe {
            // This transmute is ok because PageEntry and PageHandle have
            // exactly the same memory layout.
            std::mem::transmute(evicted)
        }
    }

    /// Evict some LRU dirty pages with a fd.
    ///
    /// Note that the results may contain false positives.
    pub fn evict_dirty_pages_by_fd(&self, fd: i32, max_count: usize) -> Vec<PageHandle> {
        let mut lru_dirty_list = self.acquire_lru_list(LruListName::Dirty);
        let cond = |entry: &PageEntryInner| entry.fd() == fd;
        let evicted: Vec<PageEntry> = lru_dirty_list.evict_nr_with(max_count, cond);
        unsafe {
            // This transmute is ok because PageEntry and PageHandle have
            // exactly the same memory layout.
            std::mem::transmute(evicted)
        }
    }

    pub fn num_dirty_pages(&self) -> usize {
        let mut lru_dirty_list = self.acquire_lru_list(LruListName::Dirty);
        lru_dirty_list.len()
    }

    fn do_release(&self, handle: PageHandle, is_discard: bool) {
        let entry = handle.unwrap();
        let mut map = self.map.lock().unwrap();

        let are_users_still_holding_handles = |entry: &PageEntry| {
            let internal_refcnt = if entry.list_name().is_some() {
                2 // 1 for lru_list + 1 for map
            } else {
                1 // 1 for lru_list
            };
            let user_refcnt = PageEntry::refcnt(entry) - internal_refcnt;
            user_refcnt > 0
        };

        let dst_list_name = {
            let mut state = entry.state();
            if are_users_still_holding_handles(&entry) {
                match *state {
                    PageState::Dirty => Some(LruListName::Dirty),
                    _ => None,
                }
            } else {
                // This is the right timing to "free" a page cache
                if is_discard {
                    *state = PageState::Uninit;
                }
                if *state == PageState::Uninit {
                    map.remove(&entry.key());
                }

                match *state {
                    PageState::Uninit => Some(LruListName::Unused),
                    PageState::UpToDate => Some(LruListName::Evictable),
                    PageState::Dirty => Some(LruListName::Dirty),
                    _ => None,
                }
            }
        };
        self.reinsert_to_lru_list(entry, dst_list_name);
    }

    fn reinsert_to_lru_list(&self, entry: PageEntry, dst_list_name: Option<LruListName>) {
        let src_list_name = entry.list_name();

        entry.set_list_name(dst_list_name);
        match (src_list_name, dst_list_name) {
            (None, None) => {
                // Do nothing
            }
            (None, Some(dst_list_name)) => {
                let mut dst_list = self.acquire_lru_list(dst_list_name);
                dst_list.insert(entry);
            }
            (Some(src_list_name), None) => {
                let mut src_list = self.acquire_lru_list(src_list_name);
                src_list.remove(&entry);
            }
            (Some(src_list_name), Some(dst_list_name)) => {
                if src_list_name == dst_list_name {
                    let mut src_dst_list = self.acquire_lru_list(src_list_name);
                    src_dst_list.touch(&entry);
                } else {
                    let mut src_list = self.acquire_lru_list(src_list_name);
                    src_list.remove(&entry);
                    drop(src_list);

                    let mut dst_list = self.acquire_lru_list(dst_list_name);
                    dst_list.insert(entry);
                    drop(dst_list);
                }
            }
        }
    }

    fn touch_lru_list(&self, entry: &PageEntry) {
        let lru_list_name = match entry.list_name() {
            Some(lru_list_name) => lru_list_name,
            None => {
                return;
            }
        };
        let mut lru_list = self.acquire_lru_list(lru_list_name);
        lru_list.touch(entry);
    }

    fn evict_from_lru_list(&self, name: LruListName) -> Option<PageEntry> {
        let evicted_entry_opt = self.acquire_lru_list(LruListName::Evictable).evict();
        let evicted_entry = match evicted_entry_opt {
            Some(evicted_entry) => evicted_entry,
            None => {
                return None;
            }
        };

        // Check some invariance
        debug_assert!(match name {
            LruListName::Unused => {
                PageEntry::refcnt(&evicted_entry) == 1
                    && *evicted_entry.state() == PageState::Uninit
            }
            LruListName::Evictable => {
                PageEntry::refcnt(&evicted_entry) == 2
                    && *evicted_entry.state() == PageState::UpToDate
            }
            LruListName::Dirty => {
                PageEntry::refcnt(&evicted_entry) > 2
            }
        });

        evicted_entry.set_list_name(None);
        Some(evicted_entry)
    }

    fn acquire_lru_list(&self, name: LruListName) -> MutexGuard<PageLruList> {
        self.lru_lists[name as usize].lock().unwrap()
    }
}
