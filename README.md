# Exasol Go SQL Driver

[![Build Status](https://github.com/exasol/exasol-driver-go/actions/workflows/ci-build.yml/badge.svg)](https://github.com/exasol/exasol-driver-go/actions/workflows/ci-build.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/exasol/exasol-driver-go.svg)](https://pkg.go.dev/github.com/exasol/exasol-driver-go)

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Aexasol-driver-go&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.exasol%3Aexasol-driver-go)

[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Aexasol-driver-go&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Aexasol-driver-go)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Aexasol-driver-go&metric=bugs)](https://sonarcloud.io/dashboard?id=com.exasol%3Aexasol-driver-go)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Aexasol-driver-go&metric=code_smells)](https://sonarcloud.io/dashboard?id=com.exasol%3Aexasol-driver-go)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Aexasol-driver-go&metric=coverage)](https://sonarcloud.io/dashboard?id=com.exasol%3Aexasol-driver-go)

This repository contains a Go library for connection to the [Exasol](https://www.exasol.com/) database.

This library uses the standard Golang [SQL driver interface](https://golang.org/pkg/database/sql/) for easy use.

## Prerequisites

To use the Exasol Go Driver you need a supported Exasol database release: 2025.1.x, 8.29.x, or 2026.1.x. Older versions might work but are not tested.

## Further Information

* [User guide](doc/user_guide/user_guide.md)
* [Developer guide](doc/developer_guide.md)
* [Examples](examples)
* [Changelog](doc/changes/changelog.md)
* [Dependencies](dependencies.md)
