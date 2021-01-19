use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::RwLock;

use crate::event::waiter::{Waiter, WaiterQueue};
use crate::file::tracker::SeqRdTracker;
use crate::page_cache::{Page, PageCache, PageHandle, PageState};
use crate::util::{align_down, align_up};

pub use self::flusher::Flusher;

mod flusher;
mod tracker;

/// An instance of file with async APIs.
pub struct AsyncFile<Rt: AsyncFileRt> {
    fd: i32,
    len: RwLock<usize>,
    can_read: bool,
    can_write: bool,
    seq_rd_tracker: SeqRdTracker,
    waiter_queue: WaiterQueue,
    phantom_data: PhantomData<Rt>,
}

/// The runtime support of AsyncFile.
///
/// AsyncFile cannot work on its own: it leverages PageCache to accelerate I/O,
/// needs Flusher to persist data, and eventually depends on IoUring to perform
/// async I/O. This trait provides a common interface for user-implemented runtimes
/// that bind the runtime dependencies of AsyncFile with AsyncFile itself.
pub trait AsyncFileRt {
    /// Returns the io_uring instance.
    //fn io_uring() -> &'static IoUring;
    fn page_cache() -> &'static PageCache;
    fn flusher() -> &'static Flusher;
    fn auto_flush();
}

impl<Rt: AsyncFileRt> AsyncFile<Rt> {
    /// Open a file at a given path.ZA Z$%
    ///uy
    /// The three arguments have the same meaning as the open syscall.
    pub fn open(mut path: String, flags: i32, mode: i32) -> Result<Arc<Self>, i32> {
        let (can_read, can_write) = if flags & libc::O_RDONLY != 0 {
            (true, false)
        } else if flags & libc::O_WRONLY != 0 {
            (false, true)
        } else if flags & libc::O_RDWR != 0 {
            (true, true)
        } else {
            return Err(libc::EINVAL);
        };

        let fd = unsafe {
            path.push('\0');
            let c_path = std::ffi::CString::new(path).unwrap();
            let c_path_ptr = c_path.as_bytes_with_nul().as_ptr() as _;
            libc::open(c_path_ptr, flags, mode)
        };
        if fd < 0 {
            return Err(errno());
        }

        let len = unsafe { libc::lseek(fd, 0, libc::SEEK_END) };
        if len < 0 {
            return Err(errno());
        }

        Ok(Arc::new(Self {
            fd,
            len: RwLock::new(len as usize),
            can_read,
            can_write,
            seq_rd_tracker: SeqRdTracker::new(),
            waiter_queue: WaiterQueue::new(),
            phantom_data: PhantomData,
        }))
    }

    pub async fn read_at(self: &Arc<Self>, offset: usize, buf: &mut [u8]) -> i32 {
        // Prevent offset calculation from overflow
        if offset >= usize::max_value() / 2 {
            return -libc::EINVAL;
        }
        // Prevent the return length (i32) from overflow
        if buf.len() >= i32::max_value() as usize {
            return -libc::EINVAL;
        }

        // Fast path
        let retval = self.try_read_at(offset, buf);
        if retval != -libc::EAGAIN {
            return retval;
        }

        // Slow path
        let waiter = Waiter::new();
        self.waiter_queue.enqueue(&waiter);
        let retval = loop {
            let retval = self.try_read_at(offset, buf);
            if retval != -libc::EAGAIN {
                break retval;
            }

            waiter.wait().await;
        };
        self.waiter_queue.dequeue(&waiter);
        retval
    }

    fn try_read_at(self: &Arc<Self>, offset: usize, _buf: &mut [u8]) -> i32 {
        let file_len = *self.len.read().unwrap();

        // For reads beyond the end of the file
        if offset >= file_len {
            for b in _buf.iter_mut() {
                *b = 0;
            }
            return _buf.len() as i32;
        }
        // For reads within the bound of the file
        let file_remaining = file_len - offset;
        let buf_len = _buf.len().min(file_remaining);
        let buf = &mut _buf[..buf_len];

        // Determine if it is a sequential read and how much data to prefetch
        let seq_rd = self.seq_rd_tracker.accept(offset, buf.len());
        let prefetch_len = {
            let prefetch_len = seq_rd.as_ref().map_or(0, |seq_rd| seq_rd.prefetch_size());
            let max_prefetch_len = file_remaining - buf.len();
            prefetch_len.min(max_prefetch_len)
        };

        // Fetch the data to the page cache and copy data from the first ready pages to
        // the read buffer.
        let mut read_nbytes = 0;
        let access_first_ready_pages = |page_handle: &PageHandle| {
            let copy_offset = offset + read_nbytes - page_handle.offset();
            let copy_size = Page::size().min(buf_len - read_nbytes);
            let page_slice = unsafe { page_handle.page().as_slice() };
            let target_buf = &mut buf[copy_offset..copy_offset + copy_size];
            target_buf.copy_from_slice(&page_slice[..copy_size]);
            read_nbytes += copy_size;
        };
        self.fetch_pages(offset, buf_len, prefetch_len, access_first_ready_pages);

        if read_nbytes > 0 {
            seq_rd.map(|seq_rd| seq_rd.complete(read_nbytes));
            read_nbytes as i32
        } else {
            -libc::EAGAIN
        }
    }

    fn fetch_pages(
        self: &Arc<Self>,
        offset: usize,
        len: usize,
        prefetch_len: usize,
        mut access_fn: impl FnMut(&PageHandle),
    ) {
        let page_cache = Rt::page_cache();
        let mut consecutive_pages = Vec::new();
        let offset_end = align_up(offset + len + prefetch_len, Page::size());
        let offset_begin = align_down(offset, Page::size());
        let fetch_end = align_up(offset + len, Page::size());
        let mut is_fetching = true;
        for offset in (offset_begin..offset_end).step_by(Page::size()) {
            if is_fetching && offset >= fetch_end {
                is_fetching = false;
            }

            let page = page_cache.acquire(self.fd, offset).unwrap();
            let mut state = page.state();
            if is_fetching {
                // The fetching phase
                match *state {
                    PageState::UpToDate | PageState::Dirty | PageState::Flushing => {
                        // Invoke the access function
                        (access_fn)(&page);

                        drop(state);
                        page_cache.release(page);
                    }
                    PageState::Uninit => {
                        // Start prefetching
                        *state = PageState::Fetching;
                        drop(state);
                        consecutive_pages.push(page);

                        // Transit to the prefetching phase
                        is_fetching = false;
                    }
                    PageState::Fetching => {
                        // We do nothing here
                        drop(state);
                        page_cache.release(page);

                        // Transit to the prefetching phase
                        is_fetching = false;
                    }
                }
            } else {
                // The prefetching phase
                match *state {
                    PageState::Uninit => {
                        // Add one more page to prefetch
                        *state = PageState::Fetching;
                        drop(state);
                        consecutive_pages.push(page);
                    }
                    _ => {
                        drop(state);
                        page_cache.release(page);

                        // When reaching the end of consecutive pages, start the I/O
                        if consecutive_pages.len() > 0 {
                            self.read_consecutive_pages(consecutive_pages);
                        }
                        consecutive_pages = Vec::new();
                    }
                }
            }
        }
        // When reaching the end of consecutive pages, start the I/O
        if consecutive_pages.len() > 0 {
            self.read_consecutive_pages(consecutive_pages);
        }
    }

    fn read_consecutive_pages(self: &Arc<Self>, consecutive_pages: Vec<PageHandle>) {
        let first_offset = consecutive_pages[0].offset();
        let self_ = self.clone();
        let iovecs = consecutive_pages
            .iter()
            .map(|page_handle| libc::iovec {
                iov_base: page_handle.page().as_mut_ptr() as _,
                iov_len: Page::size(),
            })
            .collect::<Vec<libc::iovec>>();
        let callback = move |retval| {
            let page_cache = Rt::page_cache();
            let read_nbytes = if retval >= 0 { retval } else { 0 } as usize;
            for page in consecutive_pages {
                {
                    let mut state = page.state();
                    debug_assert!(*state == PageState::Fetching);
                    if page.offset() + Page::size() < first_offset + read_nbytes {
                        *state = PageState::UpToDate;
                    } else {
                        *state = PageState::Uninit;
                    }
                }
                page_cache.release(page);
            }
            //self_.waiter_queue.wake_all();
        };
        //let io_uring = self.io_uring();
        //let handle = io_uring::readv(fd, first_offset, iovecs.as_mut_ptr(), iovecs.len(), callback);
        //drop(handle);
    }

    pub async fn write_at(self: &Arc<Self>, offset: usize, buf: &[u8]) -> i32 {
        // Fast path
        let retval = self.try_write(offset, buf);
        if retval != -libc::EAGAIN {
            return retval;
        }

        // Slow path
        let waiter = Waiter::new();
        self.waiter_queue.enqueue(&waiter);
        let retval = loop {
            let retval = self.try_write(offset, buf);
            if retval != -libc::EAGAIN {
                break retval;
            }

            waiter.wait().await;
        };
        self.waiter_queue.dequeue(&waiter);
        retval
    }

    fn try_write(self: &Arc<Self>, offset: usize, buf: &[u8]) -> i32 {
        // Prevent offset calculation from overflow
        if offset >= usize::max_value() / 2 {
            return -libc::EINVAL;
        }
        // Prevent the return length (i32) from overflow
        if buf.len() >= i32::max_value() as usize {
            return -libc::EINVAL;
        }

        let mut new_dirty_pages = false;
        let mut write_nbytes = 0;
        let page_cache = Rt::page_cache();
        let offset_end = align_up(offset + buf.len(), Page::size());
        let offset_begin = align_down(offset, Page::size());
        for page_offset in (offset_begin..offset_end).step_by(Page::size()) {
            let copy_offset = offset + write_nbytes - page_offset;
            let copy_size = Page::size().min(buf.len() - write_nbytes);
            let to_write_full_page = copy_offset == 0 && copy_size == Page::size();

            let mut do_write = |page_handle: &PageHandle| {
                let page_slice = unsafe { page_handle.page().as_slice_mut() };
                let src_buf = &buf[write_nbytes..write_nbytes + copy_size];
                page_slice[copy_offset..copy_offset + copy_size].copy_from_slice(src_buf);
                write_nbytes += copy_size;
            };

            let page = page_cache.acquire(self.fd, page_offset).unwrap();
            let mut state = page.state();
            match *state {
                PageState::UpToDate => {
                    (do_write)(&page);

                    *state = PageState::Dirty;
                    new_dirty_pages = true;
                    drop(state);
                    page_cache.release(page);
                }
                PageState::Uninit if to_write_full_page => {
                    (do_write)(&page);

                    *state = PageState::Dirty;
                    new_dirty_pages = true;
                    drop(state);
                    page_cache.release(page);
                }
                PageState::Dirty => {
                    (do_write)(&page);

                    drop(state);
                    page_cache.release(page);
                }
                PageState::Uninit => {
                    *state = PageState::Fetching;
                    drop(state);
                    page_cache.release(page);

                    break;
                }
                PageState::Fetching | PageState::Flushing => {
                    // We do nothing here
                    drop(state);
                    page_cache.release(page);

                    break;
                }
            }
        }

        if new_dirty_pages {
            Rt::auto_flush();
        }

        if write_nbytes > 0 {
            // Update file length if necessary
            let mut file_len = self.len.write().unwrap();
            if offset + write_nbytes > *file_len {
                *file_len = offset + write_nbytes;
            }

            write_nbytes as i32
        } else {
            -libc::EAGAIN
        }
    }

    pub async fn flush(&self) {
        // TODO: set a max value
        Rt::flusher().flush_by_fd(self.fd, usize::max_value()).await;
    }
}

impl<Rt: AsyncFileRt> Drop for AsyncFile<Rt> {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

fn errno() -> i32 {
    unsafe {
        //*(libc::__errno_location())
        *(libc::__error())
    }
}
