# Security Policy

## Supported Versions

This portfolio project is pre-1.0. Security fixes target the current `main`
branch.

## Reporting a Vulnerability

Do not open a public issue for leaked credentials, RPC keys, database URLs, or
other sensitive material. Report privately through the repository owner's
GitHub profile contact path.

When reporting, include:

- affected command, API route, or table
- steps to reproduce
- expected impact
- whether credentials, RPC URLs, or indexed wallet data may be exposed

## Secrets

Do not commit `.env` files or real provider keys. Use `.env.example` as the
template for local configuration and rotate any key that was accidentally
committed or shared.
