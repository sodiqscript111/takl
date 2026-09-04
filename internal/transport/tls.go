package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

type TLSFiles struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

func (t TLSFiles) Enabled() bool {
	return t.CAFile != "" || t.CertFile != "" || t.KeyFile != ""
}

func (t TLSFiles) Validate() error {
	if t.CAFile == "" || t.CertFile == "" || t.KeyFile == "" {
		return fmt.Errorf("sync tls requires ca_file, cert_file, and key_file")
	}
	return nil
}

func LoadServerTLSCredentials(files TLSFiles) (credentials.TransportCredentials, error) {
	if err := files.Validate(); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(files.CAFile)
	if err != nil {
		return nil, err
	}
	caPool := x509.NewCertPool()
	if ok := caPool.AppendCertsFromPEM(caPEM); !ok {
		return nil, fmt.Errorf("failed to parse CA file")
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	return credentials.NewTLS(cfg), nil
}

func LoadClientTLSCredentials(files TLSFiles) (credentials.TransportCredentials, error) {
	if err := files.Validate(); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(files.CAFile)
	if err != nil {
		return nil, err
	}
	caPool := x509.NewCertPool()
	if ok := caPool.AppendCertsFromPEM(caPEM); !ok {
		return nil, fmt.Errorf("failed to parse CA file")
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
	}
	return credentials.NewTLS(cfg), nil
}
