# DigitalOcean Dev Environment

The Rust runtime now treats the DigitalOcean VM as a first-class development
environment descendant of the foundation model.

## Environment Variables

These follow NanoClaw's existing TypeScript contract:

- `DROPLET_SSH_HOST`
- `DROPLET_SSH_USER`
- `DROPLET_SSH_PORT`
- `DROPLET_REPO_ROOT`
- `REMOTE_WORKER_MODE`
- `REMOTE_WORKER_ROOT`
- `REMOTE_WORKER_BOOTSTRAP_TIMEOUT_MS`
- `REMOTE_WORKER_SYNC_INTERVAL_MS`
- `REMOTE_WORKER_TUNNEL_PORT_BASE`

The Rust path also accepts the `symphony-rs` deploy naming:

- `DIGITALOCEAN_HOST`
- `DIGITALOCEAN_SSH_USER`
- `DIGITALOCEAN_SSH_PORT`
- optional `DIGITALOCEAN_REPO_ROOT`

## CLI Commands

From the repository root:

```bash
cargo run -- show-dev-env
cargo run -- prepare-dev-env
cargo run -- seed-cargo-cache
cargo run -- sync-dev-env
cargo run -- exec-dev-env CARGO_NET_OFFLINE=true cargo check --bin nanoclaw --offline
```

## Behavior

- `show-dev-env` prints the resolved DigitalOcean VM configuration.
- `prepare-dev-env` installs the remote Rust toolchain and base build packages.
- `seed-cargo-cache` rsyncs the local Cargo cache to the droplet so offline builds
  can work.
- `sync-dev-env` rsyncs the local repository to the droplet mirror path and keeps
  the remote `.git` directory aligned with the local repo.
- `exec-dev-env <command...>` syncs first, then runs the command over SSH in the
  mirrored remote repository.

## Access Pattern From `symphony-rs`

`symphony-rs` does not use a custom VM transport. It uses standard SSH to the
Droplet host with the configured deploy user, then `systemd` manages the daemon.

Operationally, that means:

```bash
ssh <deploy-user>@<droplet-host>
sudo -iu symphony
```

The first command gets you onto the VM. The second switches into the daemon's
service-user context when you need the same home directory and runtime
environment as the long-running process.

## Remote Path Resolution

The remote mirror path is derived from:

1. the GitHub `origin` remote when available, for example
   `nexus-integrated-technologies/agency`
2. otherwise `local/<repo-folder-name>`

That slug is appended to `DROPLET_REPO_ROOT`.
The resulting path is a git-backed working copy, not just a file mirror.

## SSH Behavior

SSH multiplexing is off by default so the dev-environment path still works when
local `/tmp` is constrained. If you want control sockets, opt in with:

```bash
export NANOCLAW_SSH_MULTIPLEXING=1
```

## Cargo Registry Reachability

This droplet currently times out reaching `https://index.crates.io`, so Rust
builds should use the seeded local cache and offline mode:

```bash
cargo run -- seed-cargo-cache
cargo run -- exec-dev-env CARGO_NET_OFFLINE=true cargo check --bin nanoclaw --offline
```

## Foundation Lineage

The DigitalOcean VM is represented as a `DevelopmentEnvironment` in
`src/foundation/domain.rs`.
The bootstrap path emits:

- a `DevelopmentEnvironmentResolved` foundation event
- a `DevelopmentEnvironmentSnapshot` artifact

That keeps VM-backed execution inside the same descendant graph as groups,
tasks, messages, and artifacts.
