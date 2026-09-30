# TLS Configuration

We recommend to always enable TLS encryption. This is on by default, but you can enable it explicitly via driver property `encryption=1` or `config.Encryption(true)`. Please note that starting with version 8, Exasol does not support unencrypted connections anymore, so you can't use `encryption=0` or `config.Encryption(false)`.

There are two driver properties that control how TLS certificates are verified: `validateservercertificate` and `certificatefingerprint`. You have these three options depending on your setup:

* With `validateservercertificate=1` (or `config.ValidateServerCertificate(true)`) the driver will return an error for any TLS errors (e.g., unknown certificate or invalid hostname).

    Use this when the database has a CA-signed certificate. This is the default behavior.
* With `validateservercertificate=1;certificatefingerprint=<fingerprint>` (or `config.ValidateServerCertificate(true).CertificateFingerprint("<fingerprint>")`) you can specify the fingerprint (i.e. the SHA256 checksum) of the server's certificate.

    This is useful when the database has a self-signed certificate with invalid hostname but you still want to verify connecting to the correct host.

    **Note:** You can find the fingerprint by first specifying an invalid fingerprint and connecting to the database. The error will contain the actual fingerprint.
* With `validateservercertificate=0` (or `config.ValidateServerCertificate(false)`) the driver will ignore any TLS certificate errors.

    Use this if the server uses a self-signed certificate and you don't know the fingerprint. **This is not recommended.**
