// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

const GOOGLEAPIS_SUBMODULE_PATH: &str = "googleapis";
const BIGTABLE_V2_PROTO_PATH: &str = "google/bigtable/v2/bigtable.proto";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=build.rs");

    tonic_prost_build::configure()
        // server codegen is only used by the test mock server, see the
        // `test-server` feature.
        .build_server(std::env::var_os("CARGO_FEATURE_TEST_SERVER").is_some())
        .compile_protos(
            &[Path::new(GOOGLEAPIS_SUBMODULE_PATH).join(BIGTABLE_V2_PROTO_PATH)],
            &[GOOGLEAPIS_SUBMODULE_PATH.into()],
        )?;

    Ok(())
}
