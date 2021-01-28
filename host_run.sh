#!/bin/bash
cd async-file
RUSTFLAGS="--cfg use_slab --cfg use_enter_thread" cargo t
