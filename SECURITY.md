# Security Policy

## Supported versions

ChopFlow is an experimental project. Security fixes are applied to the latest
`main` and the most recent release only.

| Version | Supported          |
|---------|--------------------|
| latest  | ✅                  |
| < latest| ❌ (upgrade first)  |

## Reporting a vulnerability

If you discover a security vulnerability in ChopFlow, **please do not open a
public GitHub issue**. Instead, report it privately:

1. Open a [GitHub Security Advisory](https://github.com/ricardoleal20/ChopFlow/security/advisories/new)
   using "Report a vulnerability", **or**
2. Email the maintainer at `ricardo [at] ricardoleal20.dev`.

Please include:
- A description of the issue and its potential impact.
- Steps to reproduce (a minimal example is ideal).
- The affected version / commit.
- Any suggested fix, if you have one.

We will acknowledge receipt within **72 hours** and aim to provide an initial
assessment within **7 days**. Please give us a reasonable window to issue a fix
before any public disclosure — we will coordinate a disclosure date with you.

## Scope

In scope:
- The broker, worker, CLI, and core library in this repository.
- The gRPC and HTTP/JSON API surface.
- The embedded dashboard's interaction with the broker API.

Out of scope (but still welcome as regular issues):
- Vulnerabilities in dependencies — report upstream, then open an issue here so
  we can pin a fixed version.
- Self-hosted deployment misconfiguration (e.g. binding to `0.0.0.0` without
  auth) unless it stems from an insecure default in the code.

## Hardening notes for deployments

ChopFlow currently has **no built-in authentication or authorization** on its
gRPC or HTTP API. It is designed to run on a trusted internal network or behind
a reverse proxy that enforces auth. Do **not** expose the broker directly to the
public internet without an auth layer in front.
