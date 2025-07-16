This crate provides a general-purpose client library for Google Cloud Bigtable via gRPC.

The main entry point is the `BigTableClient`, a convenient wrapper that exposes utility methods for common Bigtable operations and automatically manages authentication by renewing tokens as needed. This makes it easy and safe to interact with Bigtable in most applications.

For advanced use cases, you can use the raw gRPC Bigtable client generated from the proto files, giving you full access to all native gRPC methods and customization options.

## Features

- **Read and Write Operations:**
  Supports reading, writing, and reverse scanning of Bigtable data.
- **Batch Operations:**
  Efficiently performs batch reads and writes to optimize throughput and minimize latency.
- **Metrics:**
  Provides detailed metrics on database operations via Prometheus integration.
- **Local and Remote Modes:**
  Seamlessly connects to either a local Bigtable emulator for development or a remote Google Cloud Bigtable instance for production use.

## Protocol Buffers

Before building this crate, you must have the `protoc` Protocol Buffers compiler installed. This is required by the `build.rs` script to generate Rust code from the `.proto` files.

- **Linux (apt or apt-get):**
  ```sh
  sudo apt install -y protobuf-compiler
  ```
- **macOS (Homebrew):**
  ```sh
  brew install protobuf
  ```

If you encounter build errors related to missing generated files, ensure that `protoc` is installed and available in your `PATH`.

## Setup

### Remote Development

To instantiate a `BigTableClient` for communicating with a remote Google Cloud Bigtable instance, you need the following:

- **Instance ID:**
  This can be found in the Google Cloud Console (e.g., `my-instance-id`).

- **Credentials:**
  Download your service account credentials JSON file from the Google Cloud Console. Store this file securely on your local machine.
  The client will use this file via the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
  Example:
  ```sh
  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/my-credentials.json
  ```

### Local development

- install `gcloud` CLI tool: https://cloud.google.com/sdk/docs/install

- install the `cbt` CLI tool

  ```sh
  gcloud components install cbt
  ```

- start the emulator

  ```sh
  gcloud beta emulators bigtable start
  ```

- set `BIGTABLE_EMULATOR_HOST` environment variable

  ```sh
  $(gcloud beta emulators bigtable env-init)
  ```

### Formatting

**Rust**

In order to use the unstable features specified in rustfmt.toml, you must have the correct nightly toolchain component
installed.

```sh
rustup toolchain install nightly --component rustfmt --allow-downgrade
```

This can be used regardless of the default toolchain to format the code using the following command.

```sh
cargo +nightly fmt
```
