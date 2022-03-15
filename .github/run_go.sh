#!/bin/bash

working_dir=$PWD
echo $working_dir
pushd go-client
go test ./...
popd
cp -u target/release/libtantivy_jpc.so go-client/tantivy/packaged/lib/linux-amd64/
cp -u target/tantivy_jpc.h go-client/tantivy/packaged/include/
