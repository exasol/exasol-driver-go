# Connection String

The Exasol Go SQL Driver uses the following URL format:

`exa:<host>[,<host_1>]...[,<host_n>]:<port>[;<prop_1>=<value_1>]...[;<prop_n>=<value_n>]`

Host-Range-Syntax is supported (e.g. `exasol1..3`). A range like `exasol1..exasol3` is not valid.

## Supported Driver Properties

| Property                    | Value         | Default     | Description                                     |
| :-------------------------- | :-----------: | :---------: | :---------------------------------------------- |
| `autocommit`                |  0=off, 1=on  | `1`         | Switch autocommit on or off.                    |
| `certificatefingerprint`    |  string       |             | Expected fingerprint of the server's TLS certificate. See [TLS Configuration](tls.md) for details. |
| `clientname`                |  string       | `Go client` | Tell the server the application name.           |
| `clientversion`             |  string       |             | Tell the server the version of the application. |
| `compression`               |  0=off, 1=on  | `0`         | Switch data compression on or off.              |
| `encryption`                |  0=off, 1=on  | `1`         | Switch automatic encryption on or off.          |
| `fetchsize`                 | numeric, >0   | `128`       | Amount of data in kB which should be obtained by Exasol during a fetch. The application can run out of memory if the value is too high. |
| `localimportencryption`     |  0=off, 1=on  | `1`         | **Deprecated.** Encrypt the proxy connection used for a local CSV or Parquet import when supported by the server. Set to `0` only when plaintext is explicitly required. See [Import Local Parquet Files](import_local_files.md#importing-local-parquet-files). |
| `password`                  |  string       |             | Exasol password.                                |
| `resultsetmaxrows`          |  numeric      |             | Set the max amount of rows in the result set.   |
| `schema`                    |  string       |             | Exasol schema name.                             |
| `user`                      |  string       |             | Exasol username.                                |
| `validateservercertificate` |  0=off, 1=on  | `1`         | TLS certificate verification. Disable it if you want to use a self-signed or invalid certificate (server side). |
