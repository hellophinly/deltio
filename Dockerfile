FROM rust:1.81 AS build

# Install Protocol Buffers.
RUN apt-get update && apt-get install -y protobuf-compiler clang musl-tools musl-dev

# Create a new empty project.
RUN cargo new --bin deltio
WORKDIR /deltio

# Determine the musl target for the current architecture.
RUN <<EOF
  set -e;

  touch .target
  ARCH=$(uname -m)

  if [ "$ARCH" = "x86_64" ]; then
    rustup target add x86_64-unknown-linux-musl
    echo -n "x86_64-unknown-linux-musl" > .target
  elif [ "$ARCH" = "aarch64" ]; then
    rustup target add aarch64-unknown-linux-musl
    echo -n "aarch64-unknown-linux-musl" > .target
  fi
EOF

# Copy manifests.
COPY ./.cargo/config.toml ./.cargo/config.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# Build the shell project first to get a dependency cache.
RUN <<EOF
  set -e;

  TARGET=$(cat .target)
  export CC="clang"
  export CXX="clang++"

  if [ -z "$TARGET" ]; then
    cargo build --release
    rm ./target/release/deps/deltio*
  else
    cargo build --target "$TARGET" --release
    rm ./target/*/release/deps/deltio*
  fi

  # Remove the shell project's code files.
  rm src/*.rs
EOF

# Copy the actual source.
COPY ./build.rs ./build.rs
COPY ./proto ./proto
COPY ./src ./src

# Build for release
RUN <<EOF
  set -e;

  TARGET=$(cat .target)
  export CC="clang"
  export CXX="clang++"

  if [ -z "$TARGET" ]; then
    cargo build --release
    exit 0
  fi

  cargo build --target "$TARGET" --release
  mv "target/$TARGET/release/deltio" "target/release/deltio"
EOF

# Our final base image.
FROM scratch AS deltio

# Copy the build artifact from the build stage
COPY --from=build /deltio/target/release/deltio .

# Expose the default port.
EXPOSE 8085

# Set the startup command to run the binary.
CMD ["./deltio", "--bind", "0.0.0.0:8085"]
