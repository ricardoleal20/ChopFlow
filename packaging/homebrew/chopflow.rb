# ChopFlow Homebrew formula (REFERENCE / TEMPLATE)
#
# This copy lives in the ChopFlow repo for review. The authoritative formula
# is generated and pushed to the tap repo (ricardoleal20/homebrew-chopflow) by
# the `bump-homebrew` job in .github/workflows/release.yml after each crates.io
# publish — that job fills in the real `sha256` and `version` from the published
# source tarball.
#
# Install (once the tap exists):
#   brew tap ricardoleal20/chopflow
#   brew install chopflow
#
# The formula builds from source via `cargo install --locked --path .` against
# the crates.io source tarball. The `chopflow` umbrella crate's path
# dependencies are rewritten to registry dependencies at `cargo publish` time,
# so the source build resolves everything from crates.io.

class Chopflow < Formula
  desc "Durable distributed task queue built in Rust"
  homepage "https://github.com/ricardoleal20/ChopFlow"
  url "https://crates.io/api/v1/crates/chopflow/0.1.2/download"
  sha256 "0000000000000000000000000000000000000000000000000000000000000000"
  license "Apache-2.0"
  version "0.1.2"

  # The umbrella crate is binary-only (three [[bin]] targets), so it carries a
  # Cargo.lock; building with --locked is reproducible.
  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args
  end

  test do
    assert_match "chopflow-broker", shell_output("#{bin}/chopflow-broker --help")
    assert_match "chopflow-cli", shell_output("#{bin}/chopflow-cli --help")
    assert_match "chopflow-mcp", shell_output("#{bin}/chopflow-mcp --help")
  end
end
