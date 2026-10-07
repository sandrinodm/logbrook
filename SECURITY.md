# Security policy

## Report a vulnerability

Use GitHub private vulnerability reporting for suspected security issues. In this repository, open **Security**, select **Advisories**, then choose **Report a vulnerability**. The report is shared privately with the maintainers.

If that button is unavailable, open an issue asking the maintainers to enable private reporting. Include no vulnerability details, exploit code, credentials, or sensitive logs in that public issue. GitHub's [private reporting guide](https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/report-privately) explains the workflow.

Include the following in the private report:

- The affected version or commit and deployment configuration.
- Steps to reproduce, preferably with synthetic data and a minimal proof of concept.
- The expected security boundary and how it can be bypassed.
- The impact and any known mitigation.

Do not test against installations you do not own or have permission to assess. Use the private advisory to coordinate a fix and disclosure with the maintainers.

## Release status

Logbrook is pre-release software. Include an exact commit in reports about unreleased builds. Older development snapshots do not have a separate security maintenance policy.

## Deployment boundaries

Logbrook serves plain HTTP. Use a TLS-terminating reverse proxy for remote access, keep management credentials separate from ingestion and read credentials, and scope producer and reader tokens to the indexes and sources they need. Treat application logs and database backups as sensitive data.

See the [operations guide](docs/OPERATIONS.md) for configuration, backups, and upgrade procedures, and the [container guide](docs/CONTAINER.md) for runtime permissions and resource limits.
