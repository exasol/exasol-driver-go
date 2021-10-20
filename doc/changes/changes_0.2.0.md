# Exasol Go SQL Driver 0.2.0, released 2021-10-20

Code name: Verify TLS Certificate Fingerprints

## Summary

This release contains a breaking change: We renamed the URL property `usetls` to `validateservercertificate` and method `DSNConfig.UseTLS()` to `DSNConfig.ValidateServerCertificate()`. The new names align with the [JDBC driver's property](https://docs.exasol.com/connect_exasol/drivers/jdbc.htm) `validateservercertificate`.

To migrate to the new version, you only need to rename all occurances of `usetls` to `validateservercertificate` and all occurances of `UseTLS` to `ValidateServerCertificate`. The semantic has not changed, so you don't need to modify the values.

We also added support for TLS certificate fingerprints, similar to the [JDBC driver](https://docs.exasol.com/connect_exasol/drivers/jdbc.htm). This is especially useful when your Exasol database uses a self-signed certificate or the certificate contains the wrong hostname. See the [documentation](../../README.md) for details.

The new release also uses [error-reporting-go](https://github.com/exasol/error-reporting-go/) to provide error messages with a unique ID, e.g. `E-EGOD-4`. This will allow us to generate a catalog of all error messages in a central location.

## Features

* #37: Verify TLS certificate fingerprints
* #42: Use [error-reporting-go](https://github.com/exasol/error-reporting-go) to generate Exasol error messages

## Bugfixes

## Refactoring

* #36: Renamed URL property `usetls` to `validateservercertificate`.
