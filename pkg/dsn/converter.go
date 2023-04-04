package dsn

import "github.com/exasol/exasol-driver-go/internal/config"

func ToInternalConfig(dsnConfig *DSNConfig) *config.Config {
	apiVersion := 2
	if dsnConfig.AccessToken != "" || dsnConfig.RefreshToken != "" {
		apiVersion = 3
	}
	return &config.Config{
		User:                      dsnConfig.User,
		Password:                  dsnConfig.Password,
		AccessToken:               dsnConfig.AccessToken,
		RefreshToken:              dsnConfig.RefreshToken,
		Host:                      dsnConfig.Host,
		Port:                      dsnConfig.Port,
		Params:                    dsnConfig.Params,
		ApiVersion:                apiVersion,
		ClientName:                dsnConfig.ClientName,
		ClientVersion:             dsnConfig.ClientVersion,
		Schema:                    dsnConfig.Schema,
		Autocommit:                *dsnConfig.Autocommit,
		FetchSize:                 dsnConfig.FetchSize,
		Compression:               *dsnConfig.Compression,
		ResultSetMaxRows:          dsnConfig.ResultSetMaxRows,
		Encryption:                *dsnConfig.Encryption,
		ValidateServerCertificate: *dsnConfig.ValidateServerCertificate,
		CertificateFingerprint:    dsnConfig.CertificateFingerprint,
	}
}
