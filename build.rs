// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

const GOOGLEAPIS_SUBMODULE_PATH: &str = "googleapis";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=build.rs");

    tonic_build::configure()
        // the server is on google side, we don't need code generation for it.
        .build_server(false)
        .compile_protos(
            &[Path::new(GOOGLEAPIS_SUBMODULE_PATH).join("google/bigtable/v2/bigtable.proto")],
            &[GOOGLEAPIS_SUBMODULE_PATH],
        )?;

    Ok(())
}
