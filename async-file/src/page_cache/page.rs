pub struct Page {
    // TODO: for SGX, this buffer needs to be allocated from a different source.
    buf: Box<[u8]>,
}

impl Page {
    pub fn new() -> Self {
        let buf = Vec::with_capacity(Page::size()).into_boxed_slice();
        Self { buf }
    }

    pub unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.as_ptr(), Self::size())
    }

    pub unsafe fn as_slice_mut(&self) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.as_mut_ptr(), Self::size())
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.buf.as_ptr() as _
    }

    pub const fn size() -> usize {
        4096
    }
}
