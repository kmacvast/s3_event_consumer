# Prebuilt standalone executables

Single-file executables for `s3_event_consumer`, checked into this repository so
they can be **downloaded directly** — one click, one file, no archive to extract
and **no Python installation** on the machine that runs them.

| Platform | Executable | Direct download |
| --- | --- | --- |
| Linux x86_64 (glibc 2.28+) | [`linux-x86_64/s3_event_consumer`](linux-x86_64/s3_event_consumer) | <https://raw.githubusercontent.com/kmacvast/s3_event_consumer/main/bin/linux-x86_64/s3_event_consumer> |
| macOS ARM64 / Apple Silicon | [`macos-arm64/s3_event_consumer`](macos-arm64/s3_event_consumer) | <https://raw.githubusercontent.com/kmacvast/s3_event_consumer/main/bin/macos-arm64/s3_event_consumer> |

Everything the program needs — the Python runtime, `confluent_kafka`,
`librdkafka` and `Pygments` — is bundled inside the executable. Configuration is
read at runtime from an external `s3_consumer_config.json`; nothing is baked in.

See the [repository README](../README.md#download-and-run) for the download,
configure and run steps, or the
[VAST deployment guide](../docs/vast-kafka-event-broker-5.4.md#51-get-the-consumer)
for the end-to-end walkthrough.

## Supported platforms

| | Linux x86_64 | macOS ARM64 |
| --- | --- | --- |
| Format | ELF 64-bit, dynamically linked, stripped | Mach-O 64-bit, thin |
| Architecture | x86-64 only | arm64 (Apple Silicon) only |
| Requirement | glibc 2.28 or newer — RHEL/Rocky/AlmaLinux 8 and 9, Ubuntu 20.04+, Debian 11+ | macOS on Apple Silicon |
| Not supported | musl (Alpine), non-x86_64 | Intel Macs |
| Code signing | n/a | ad-hoc only — not Developer ID, not notarised |

## Provenance

These executables were produced by **GitHub Actions**, not built by hand:

- **Workflow run:** [33091023706](https://github.com/kmacvast/s3_event_consumer/actions/runs/33091023706)
  — a manual (`workflow_dispatch`) run of
  [build-release.yml](../.github/workflows/build-release.yml)
- **Source commit:** `08247bc38640e70020737fa371ae95666f17dd54` on `main`
- **Built with:** PyInstaller 6.22.2 `--onefile`, Python 3.12 — inside an
  `almalinux:8` container for Linux, on a native `macos-14` runner for macOS

They are the exact executable payloads from that run. Nothing was rebuilt,
modified, stripped, re-signed or recompressed; the macOS ad-hoc signature is the
one CI produced and still validates. Both passed the workflow's smoke test:
`--help` succeeds, and running against an unedited
`s3_consumer_config.example.json` exits non-zero naming the placeholders that
still need filling in.

No version tag or GitHub Release has been published yet. When a `v*` tag is
pushed, the same workflow will attach identically built executables to a Release.

## Verifying a download

Hashes for both executables are in [`SHA256SUMS`](SHA256SUMS), with paths
relative to this directory:

```bash
shasum -a 256 -c SHA256SUMS      # macOS; use sha256sum -c on Linux
```

| Executable | Size | SHA-256 |
| --- | --- | --- |
| `linux-x86_64/s3_event_consumer` | 14,971,328 bytes | `22bd2d6c5991999841b7006907832421284b7ef39da4ed82709129ddf5255ddc` |
| `macos-arm64/s3_event_consumer` | 14,451,568 bytes | `e490a97eca57c4629868444fdd80cd03aecd11d00edf3119c868fc437f0a2535` |

## Executable permission

Git records both files as mode `100755`. Browser and raw downloads do not always
preserve the Unix executable bit, so if the downloaded file will not run:

```bash
chmod +x s3_event_consumer
```
