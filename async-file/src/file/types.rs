use std::any::Any;
use std::future::Future;
use std::task::{Context, Poll};
use std::sync::Mutex;
use std::pin::Pin;
use std::fmt::Debug;

pub trait File: /*Debug + */Sync + Send + Unpin + Any {
    fn poll_read_at(self: Pin<&Self>, cx: &mut Context<'_>, offset: usize, buf: &mut [u8]) -> Poll<i32>; 
    
//    fn poll_write_at(&self, offset: usize, buf: &[u8]) -> Poll<Output=i32>; 

//   fn poll_flush(&self) -> Poll<Output=i32>;

    fn as_any(&self) -> &dyn Any;
}

pub trait FileExt : File {
    fn read_at<'a>(&'a self, offset: usize, buf: &'a mut [u8]) -> Read<'a, Self> {
        Read::new(self, offset, buf)
    }
}

impl<F: File + ?Sized> FileExt for F {}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Read<'a, F: ?Sized> {
    file: &'a F,
    offset: usize,
    buf: &'a mut [u8],
}

impl<F: ?Sized + Unpin> Unpin for Read<'_, F> {}

impl<'a, F: File + ?Sized + Unpin> Read<'a, F> {
    fn new(file: &'a F, offset: usize, buf: &'a mut [u8]) -> Self {
        Self { file, offset, buf }
    }
}

impl<F: File + ?Sized + Unpin> Future for Read<'_, F> {
    type Output = i32;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        Pin::new(this.file).poll_read_at(cx, this.offset, this.buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    pub struct BufFile(Mutex<Vec<u8>>);

    impl File for BufFile {
        fn poll_read_at(self: Pin<&Self>, cx: &mut Context<'_>, offset: usize, buf: &mut [u8]) -> Poll<i32> {
            let mut self_buf = self.0.lock().unwrap();
            let copy_len = self_buf.len().min(buf.len());
            let src_buf = &self_buf[..copy_len];
            let dst_buf = &mut buf[..copy_len];
            dst_buf.copy_from_slice(src_buf);
            Poll::Ready(copy_len as i32)
        }
    /*   
        fn poll_write_at(&self, offset: usize, buf: &[u8]) -> Poll<Output=i32> {
            todo!()

        }

        fn poll_flush(&self) -> Poll<Output=i32> {
            todo!()
        }
    */
        fn as_any(&self) -> &dyn Any {
            self 
        }
    }

    #[test]
    fn buf_async_read() {
        async_rt::task::spawn(async {
            let content = "Hello World";
            let buf = {
                let vec = content.to_string().into_bytes();
                BufFile(Mutex::new(vec))
            };
            let mut read_buf = Vec::with_capacity(20);
            unsafe { read_buf.set_len(read_buf.capacity()); }
            let read_len = buf.read_at(0, &mut read_buf[..]).await as usize;
            assert!(&read_buf[..read_len] == content.as_bytes());
        });
    }

    #[test]
    fn dynamic_dipatch() {
        async_rt::task::spawn(async {
            let buf : Arc<dyn File>= {
                let content = "Hello World";
                let vec = content.to_string().into_bytes();
                Arc::new(BufFile(Mutex::new(vec))) 
            };
            let mut read_buf = Vec::with_capacity(20);
            unsafe { read_buf.set_len(read_buf.capacity()); }
            buf.read_at(0, &mut read_buf).await;
        });
    }
}