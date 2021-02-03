# async-io
Rust async-io crate based on Linux io_uring and Rust async / await.

Support both host and SGX.

- **async-file** crate :  async file IO.
- **async-rt** crate : Rust async / await runtime.
- **async-socket** crate : async socket IO.
- **io-uring-callback** crate : io-uring with callback interface.
- **sgx-untrusted-alloc** crate : untrusted memory allocator in SGX
- **test** : test scripts.
- **third_parties/flume** : flume crate with sgx feature.
- **third_parties/incubator-teaclave-sgx-sdk** : incubator-teaclave-sgx-sdk repo. Need clone from https://github.com/apache/incubator-teaclave-sgx-sdk.git by yourself and checkout incubator-teaclave-sgx-sdk to ```d94996``` commit.
- **third_parties/io-uring** : io-uring crate with sgx feature.

## Quick start
```
// clone async-io repo.
cd async-io
git submodule update --init
cd third_parties
git clone https://github.com/apache/incubator-teaclave-sgx-sdk.git
cd incubator-teaclave-sgx-sdk
git checkout d94996

// test
cd ../../test
./host_run_async_file_example.sh
./host_run_async_socket_example.sh
./sgx_run_async_file_example.sh
// before run sgx_run_async_socket_example.sh, you need comment out #![feature(hash_set_entry)] in async-socket/src/lib.rs
./sgx_run_async_socket_example.sh
```
