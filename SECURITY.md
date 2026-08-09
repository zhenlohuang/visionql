# Security Policy

## Supported versions

VisionQL is currently pre-1.0. Security fixes are developed on `main` and, when applicable, backported only to the latest `0.1.x` release.

| Version | Supported |
|---|---|
| `main` | Yes |
| Latest `0.1.x` | Yes |
| Older versions | No |

Pre-1.0 fixes may include compatibility changes when they are necessary to close a vulnerability.

## Report a vulnerability

Do not disclose suspected vulnerabilities in a public issue, discussion, or pull request.

Use [GitHub private vulnerability reporting](https://github.com/zhenlohuang/visionql/security/advisories/new) to send the maintainers:

- the affected version or commit;
- the impact and realistic attack scenario;
- reproduction steps or a minimal proof of concept;
- relevant operating system, FFmpeg, model source, and configuration details;
- any suggested mitigation, if known.

If private reporting is unavailable, open a public issue requesting a private contact channel without including vulnerability details.

We aim to acknowledge a report within three business days. After validation, maintainers will coordinate a fix, release, and disclosure timeline with the reporter. Please keep the report private until a fix is available.

## Scope

Security reports may include, but are not limited to:

- unsafe handling of local or remote media and model artifacts;
- path traversal or catalog integrity violations;
- unintended disclosure of media, credentials, or `HF_TOKEN`;
- request or response handling in the HTTP model endpoint;
- denial of service caused by untrusted SQL, media, model, or endpoint input;
- dependency vulnerabilities with a demonstrated impact on VisionQL.

Model accuracy, expected probabilistic inference errors, and capabilities explicitly marked as unavailable are not security vulnerabilities unless they create a concrete security impact.
