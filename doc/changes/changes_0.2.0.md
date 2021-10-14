# Exasol Go SQL Driver 0.2.0, released 2021-10-??

Code name: Improve interface

## Summary

This release contains a breaking change: We renamed the URL property `usetls` to `validateservercertificate` and `DSNConfig.UseTLS()` to `DSNConfig.ValidateServerCertificate()`. The new name aligns with the [JDBC driver's property](https://docs.exasol.com/connect_exasol/drivers/jdbc.htm) `validateservercertificate`.

To migrate to the new version, you only need to rename all occurances of `usetls` and `UseTLS` to `validateservercertificate` resp. `ValidateServerCertificate`. The semantic has not changed, so you don't need to modify the values.

## Bugfixes

## Refactoring

* #36: Renamed URL property `usetls` to `validateservercertificate`.
