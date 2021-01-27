#!/bin/bash
cd async-file
RUSTFLAGS="--cfg use_slab" cargo t