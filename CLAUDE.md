# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Terry is a personal voicemail service. It uses the 46elks telephony API to play a greeting and record a message, stores the recording in S3-compatible object storage, transcribes it with OpenAI, and sends the transcription to the owner by SMS (split into several messages if long) with a link to the recording. Modelled on henrrrik/telefonista.

# Build & Test
- `make test` — run tests with the race detector (matches CI)
- `make fmt` — run before committing (gofmt -s + goimports)
- `make lint` — run before committing; golangci-lint (pinned in the Makefile) with vet, staticcheck, gosec, gocyclo (>10) etc., config in `.golangci.yml`
- `make vulncheck` — govulncheck

# Project
- Single-package Go app (all code in root main package)
- Interfaces (`objectStore`, `transcriber`) are used for test doubles, and 46elks is faked with an httptest server via `server.elksAPI` — keep this pattern
- CI runs on PRs and main: golangci-lint, tests with race detector, govulncheck

## Workflow

- Use Red/Green TDD
- Create a PR for all changes — do not push directly to main.
