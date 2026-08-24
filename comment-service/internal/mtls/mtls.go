package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// ServerConfig loads a mTLS config for an internal gRPC server.
func ServerConfig(caFile, certFile, keyFile string, allowedClients []string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	caPool, err := loadCAPool(caFile)
	if err != nil {
		return nil, err
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS12,
	}
	if len(allowedClients) > 0 {
		allowed := nameSet(allowedClients)
		cfg.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("missing client certificate")
			}
			if !certificateMatches(state.PeerCertificates[0], allowed) {
				return fmt.Errorf("client certificate %q is not allowed", state.PeerCertificates[0].Subject.CommonName)
			}
			return nil
		}
	}
	return cfg, nil
}

func loadCAPool(caFile string) (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read ca certificate: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("append ca certificate failed")
	}
	return caPool, nil
}

func nameSet(names []string) map[string]struct{} {
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		allowed[name] = struct{}{}
	}
	return allowed
}

func certificateMatches(cert *x509.Certificate, allowed map[string]struct{}) bool {
	if _, ok := allowed[cert.Subject.CommonName]; ok {
		return true
	}
	for _, dns := range cert.DNSNames {
		if _, ok := allowed[dns]; ok {
			return true
		}
	}
	for _, uri := range cert.URIs {
		if _, ok := allowed[uri.String()]; ok {
			return true
		}
	}
	return false
}
