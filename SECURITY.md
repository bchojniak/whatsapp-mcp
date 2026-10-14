# Security policy

## Reporting a vulnerability

Do not disclose vulnerabilities, credentials, QR codes, session databases, private
messages, or media in a public issue, pull request, or log attachment.

For public releases, use **Security → Report a vulnerability** on the repository
to send a private report. If that option is unavailable, ask the maintainer to
enable private reporting without disclosing the vulnerability. While the
repository is private, use the private contact channel through which access was
provided.

Include the affected commit or version, connection mode, a description of the
impact, and reproduction steps using synthetic data. Remove keys, personal
identifiers, messages, and session material from any supporting output.

## Supported versions

Security fixes target the latest `main` revision. Older commits and downstream
copies do not receive separate backports. Review updates and retain consistent
backups before upgrading a paired worker.

## Security boundaries

Every MCP HTTP request requires a client key. Tool permissions are enforced
before execution; sending also requires the service setting and an explicit
client grant. These permissions cover the stored archive rather than individual
conversations. Shared client keys do not identify individual end users.

The WhatsApp bridge and pairing endpoint are private internal services. Keep
them on loopback. Docker Compose publishes only the authenticated MCP port and
binds it to the host's loopback interface. Remote access requires a private route
such as Tailscale or an OpenAI tunnel.

The persistent volume contains readable WhatsApp content and linked-device keys.
Anyone with access to that volume, backups, or worker administration can access
them. File permissions and a private network are not application-level
encryption at rest. Treat a copied session as account access.

See [client authentication](CLIENT_AUTH.md), [text sending](docs/SENDING.md),
and [publication preparation](docs/PUBLISHING.md).
