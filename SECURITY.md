# Security Policy

## Supported versions

Only the latest release line of the `@amqp-contract/*` packages receives security fixes.

## Reporting a vulnerability

Please **do not open a public issue**. Report it privately through
[GitHub private vulnerability reporting](https://github.com/btravstack/amqp-contract/security/advisories/new).

Include the affected package and version, a description of the impact, and steps to reproduce.
You can expect an acknowledgement within a few days. Fixes ship as a patch release with a
GitHub Security Advisory crediting the reporter unless you ask otherwise.

## Release integrity

Packages are published from CI only, via npm Trusted Publishing, with
[provenance](https://docs.npmjs.com/generating-provenance-statements). Verify with
`npm audit signatures`.
