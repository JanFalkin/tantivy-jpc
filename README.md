# c-api access to Tantivy Search using JPC 1.0

## Installing

### Install Golang > 1.16

https://go.dev/dl/

### Install Rust

```
curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env
```
## Building


### Rust
```
cargo build

```
This will create a symbolic link inside the go-client directory to assist in linking the sample

### Golang

```
cd go-client
RUST_LOG=info go run ./...

```

