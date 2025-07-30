package connection

import (
	"context"
	"os"

	"github.com/exasol/exasol-driver-go/internal/utils"
	"github.com/exasol/exasol-driver-go/pkg/proxy"
)

type ImportStatement struct {
	query string
	host  string
	port  int
	proxy *proxy.Proxy
}

func NewImportStatement(query string, host string, port int) (*ImportStatement, error) {
	p, err := createProxy(host, port)
	if err != nil {
		return nil, err
	}
	err = p.StartProxy()
	if err != nil {
		p.Close()
		return nil, err
	}
	return &ImportStatement{query: query, host: host, port: port, proxy: p}, nil
}

func createProxy(host string, port int) (*proxy.Proxy, error) {
	hosts, err := utils.ResolveHosts(host)
	if err != nil {
		return nil, err
	}
	utils.ShuffleHosts(hosts)
	return proxy.NewProxy(hosts, port)
}

func (i *ImportStatement) GetUpdatedQuery() string {
	return utils.UpdateImportQuery(i.query, i.proxy.Host, i.proxy.Port)
}

func (i *ImportStatement) Close() {
	i.proxy.Close()
}

func (i *ImportStatement) UploadFiles(ctx context.Context) error {
	paths, err := utils.GetFilePaths(i.query)
	if err != nil {
		return err
	}

	var files []*os.File
	for _, path := range paths {
		f, ferr := utils.OpenFile(path)
		if ferr != nil {
			return ferr
		}
		files = append(files, f)
	}

	err = i.proxy.Write(ctx, files, utils.GetRowSeparator(i.query))
	if err != nil {
		return err
	}

	return nil
}
