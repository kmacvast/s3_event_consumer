# Prebuilt standalone executables

Ready-to-run builds of `s3_event_consumer`, checked in so you can download one
directly from this repository without a GitHub Release, a CI login, or a Python
installation on the target machine.

| Platform | Archive |
| --- | --- |
| Linux x86_64 (glibc 2.28+) | [`s3_event_consumer-linux-x86_64.tar.gz`](s3_event_consumer-linux-x86_64.tar.gz) |
| macOS Apple Silicon (arm64) | [`s3_event_consumer-macos-arm64.tar.gz`](s3_event_consumer-macos-arm64.tar.gz) |

Each archive contains the executable and a copy of
`s3_consumer_config.example.json`. See the
[repository README](../README.md#option-a-standalone-executable-no-python-needed)
for download, extraction and configuration steps, or the
[VAST deployment guide](../docs/vast-kafka-event-broker-5.4.md#51-get-the-consumer)
for the end-to-end walkthrough.

These are the intended **first public, demo-ready binaries** for the project.

## Provenance

These are the **unmodified artifacts** from GitHub Actions run
[33091023706](https://github.com/kmacvast/s3_event_consumer/actions/runs/33091023706),
a manual (`workflow_dispatch`) run of
[build-release.yml](../.github/workflows/build-release.yml) against source commit
`08247bc38640e70020737fa371ae95666f17dd54` on `main`.

Nothing was rebuilt, re-signed, recompressed or repacked. The archive **contents
and bytes are exactly as CI produced them** — only the repository filenames
differ. CI names untagged builds `s3_event_consumer-dev-<short-sha>-<platform>.tar.gz`;
they are checked in here under stable, customer-facing names so download links
stay valid across rebuilds:

| CI artifact filename | Filename in this directory |
| --- | --- |
| `s3_event_consumer-dev-08247bc-linux-x86_64.tar.gz` | `s3_event_consumer-linux-x86_64.tar.gz` |
| `s3_event_consumer-dev-08247bc-macos-arm64.tar.gz` | `s3_event_consumer-macos-arm64.tar.gz` |

A future `v*` tag will build identically and attach tag-named archives to a
GitHub Release.

| | Linux x86_64 | macOS arm64 |
| --- | --- | --- |
| Build environment | `almalinux:8` container | `macos-14` runner |
| Python | 3.12 (distro, shared `libpython`) | 3.12 (`actions/setup-python`) |
| PyInstaller | 6.22.2, `--onefile` | 6.22.2, `--onefile` |
| Code signing | n/a | ad-hoc (not Developer ID, not notarised) |

Both builds passed the workflow's smoke test: `--help` succeeds, and running
against the unedited `s3_consumer_config.example.json` exits non-zero naming the
placeholders that still need filling in.

## Verifying a download

```bash
shasum -a 256 -c SHA256SUMS
```

Expected digests:

```
67031d9512c34b69eb00c781f2c9685df69c65bdaa31c499122094a0eefacdce  s3_event_consumer-linux-x86_64.tar.gz
62a1176ca6f0337264c4d87b2372c6bf9e37e4e79aef45c08a06aa8d05f41fbb  s3_event_consumer-macos-arm64.tar.gz
```

On Linux use `sha256sum -c SHA256SUMS` instead.
