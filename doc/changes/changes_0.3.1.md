# Exasol Go SQL Driver 0.3.1, released 2022-03-30

Code name: Dependency Updates

## Summary

Update dependencies to fix security issues:
* [GHSA-qq97-vm5h-rrhg](https://github.com/advisories/GHSA-qq97-vm5h-rrhg)
* [GHSA-crp2-qrr5-8pq7](https://github.com/advisories/GHSA-crp2-qrr5-8pq7)
* [CVE-2021-43784](https://github.com/advisories/GHSA-v95c-p5hm-xq8f)
* [CVE-2022-23648](https://github.com/advisories/GHSA-crp2-qrr5-8pq7)

## Dependency Updates

### Direct Dependencies

* Updated `github.com/exasol/error-reporting-go v0.1.0` to `v1.1.1`
* Updated `github.com/testcontainers/testcontainers-go v0.12.0` to `v0.12.1`

### Indirect Dependencies

* Added `github.com/containerd/containerd v1.6.2`
* Added `github.com/docker/distribution v2.8.1+incompatible`
* Added `github.com/opencontainers/runc v1.1.1`
* Added `github.com/docker/docker v20.10.14+incompatible`
