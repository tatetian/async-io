use super::*;
use sgx_trts::libc;
use std::prelude::v1::*;
use async_file::*;
use crate::runtime::Runtime;

pub async fn test() {
    let path = "tmp.data";
    let file = {
        let path = path.to_string();
        let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
        let mode = libc::S_IRUSR | libc::S_IWUSR;
        AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
    };
    let input_buf = "hello world\n".to_string().into_bytes().into_boxed_slice();
    let retval = file.write_at(0, &input_buf).await;
    assert!(retval as usize == input_buf.len());
    file.flush().await;
    drop(file);

    let file = {
        let path = path.to_string();
        let flags = libc::O_RDONLY;
        let mode = 0;
        AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
    };
    let mut output_vec = Vec::with_capacity(input_buf.len());
    output_vec.resize(input_buf.len(), 0);
    let mut output_buf = output_vec.into_boxed_slice();
    let retval = file.read_at(0, &mut output_buf[..]).await;
    assert!(retval as usize == input_buf.len());
    assert!(output_buf.len() == input_buf.len());
    assert!(output_buf == input_buf);
    println!("hello_world test success");
}