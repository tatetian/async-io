use super::*;
use sgx_trts::libc;
use std::prelude::v1::*;
use std::time::{Duration, Instant};
use async_file::*;
use async_rt::task::JoinHandle;
use crate::runtime::Runtime;

const FILE_LEN: usize = 4096 * 10240; // 40 MB
async fn prepare_file(file_num: usize) {
    for idx in 0..file_num {
        let file = {
            let path = format!("tmp.data.{}", idx).to_string();
            let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
            let mode = libc::S_IRUSR | libc::S_IWUSR;
            AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
        };

        let mut data : Vec<u8> = vec![0; FILE_LEN];
        let input_buf = data.into_boxed_slice();
        let retval = file.write_at(0, &input_buf).await;
        assert!(retval as usize == input_buf.len());
        file.flush().await;
    }
}

pub async fn test() {
    let file_num: usize = 5;
    let block_size: usize = 4096 * 8;
    prepare_file(file_num).await;
    
    let start = Instant::now();
    let mut join_handles: Vec<JoinHandle<i32>> = (0..file_num)
        .map(|i| {
            async_rt::task::spawn(async move {
                let file = {
                    let path = format!("tmp.data.{}", i).to_string();
                    let flags = libc::O_RDWR;
                    let mode = 0;
                    AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
                };

                let mut vec = vec![0; block_size];
                let mut buf = vec.into_boxed_slice();
                let mut offset = 0;
                while offset < FILE_LEN {
                    let retval = file.read_at(offset, &mut buf[..]).await;
                    assert!(retval >= 0);
                    offset += retval as usize;
                }
                
                i as i32
            })
        })
        .collect();

    for (i, join_handle) in join_handles.iter_mut().enumerate() {
        assert!(join_handle.await == (i as i32));
    }

    let duration = start.elapsed();
    println!("sequential read [file_size: {}, file_num: {}, block_size: {}] is: {:?}, throughput: {} Mb/s", 
            FILE_LEN, file_num, block_size, duration, ((FILE_LEN * file_num) as f64 / 1024.0 / 1024.0) / (duration.as_millis() as f64 / 1000.0));



    let start = Instant::now();
    let mut join_handles: Vec<JoinHandle<i32>> = (0..file_num)
        .map(|i| {
            async_rt::task::spawn(async move {
                let file = {
                    let path = format!("tmp.data.{}", i).to_string();
                    let flags = libc::O_RDWR;
                    let mode = 0;
                    AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
                };
    
                let mut vec = vec![0; block_size];
                let mut buf = vec.into_boxed_slice();
                let mut offset = 0;
                while offset < FILE_LEN {
                    buf[0] = buf[0] % 128 + 1;
                    let retval = file.write_at(offset, &buf[..]).await;
                    assert!(retval >= 0);
                    offset += retval as usize;
                }
    
                file.flush().await;

                i as i32
            })
        })
        .collect();
    
    for (i, join_handle) in join_handles.iter_mut().enumerate() {
        assert!(join_handle.await == (i as i32));
    }
    
    let duration = start.elapsed();
    println!("sequential write [file_size: {}, file_num: {}, block_size: {}] is: {:?}, throughput: {} Mb/s", 
            FILE_LEN, file_num, block_size, duration, ((FILE_LEN * file_num) as f64 / 1024.0 / 1024.0) / (duration.as_millis() as f64 / 1000.0));
}


pub async fn test2() {
    let file_num: usize = 5;
    let block_size: usize = 4096 * 1;
    prepare_file(file_num).await;
    
    let start = Instant::now();
    let mut join_handles: Vec<JoinHandle<i32>> = (0..file_num)
        .map(|i| {
            async_rt::task::spawn(async move {
                let file = {
                    let path = format!("tmp.data.{}", i).to_string();
                    let flags = libc::O_RDWR;
                    let mode = 0;
                    AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
                };

                let mut vec = vec![0; block_size];
                let mut buf = vec.into_boxed_slice();
                let mut offset = 0;
                while offset < FILE_LEN {
                    let retval = file.read_at(offset, &mut buf[..]).await;
                    assert!(retval >= 0);
                    offset += retval as usize;
                }

                offset = 0;
                while offset < FILE_LEN {
                    buf[0] = buf[0] % 128 + 1;
                    let retval = file.write_at(offset, &buf[..]).await;
                    assert!(retval >= 0);
                    offset += retval as usize;
                }

                file.flush().await;
                
                
                i as i32
            })
        })
        .collect();

    for (i, join_handle) in join_handles.iter_mut().enumerate() {
        assert!(join_handle.await == (i as i32));
    }

    let duration = start.elapsed();
    println!("sequential [file_size: {}, file_num: {}, block_size: {}] is: {:?}, throughput: {} Mb/s", 
            FILE_LEN, file_num, block_size, duration, ((FILE_LEN * file_num) as f64 / 1024.0 / 1024.0) / (duration.as_millis() as f64 / 1000.0));
}