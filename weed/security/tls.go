package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/tls/certprovider/pemfile"
	"google.golang.org/grpc/security/advancedtls"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

const CredRefreshingInterval = time.Duration(5) * time.Hour

type Authenticator struct {
	AllowedWildcardDomain string
	AllowedCommonNames    map[string]bool
}

func LoadServerTLS(config *util.ViperProxy, component string) (grpc.ServerOption, grpc.ServerOption) {
	if config == nil {
		return nil, nil
	}

	serverOptions := pemfile.Options{
		CertFile:        config.GetString(component + ".cert"),
		KeyFile:         config.GetString(component + ".key"),
		RefreshDuration: CredRefreshingInterval,
	}
	if serverOptions.CertFile == "" || serverOptions.KeyFile == "" {
		return nil, nil
	}

	serverIdentityProvider, err := pemfile.NewProvider(serverOptions)
	if err != nil {
		glog.Warningf("pemfile.NewProvider(%v) %v failed: %v", serverOptions, component, err)
		return nil, nil
	}

	serverRootOptions := pemfile.Options{
		RootFile:        config.GetString("grpc.ca"),
		RefreshDuration: CredRefreshingInterval,
	}
	serverRootProvider, err := pemfile.NewProvider(serverRootOptions)
	if err != nil {
		glog.Warningf("pemfile.NewProvider(%v) failed: %v", serverRootOptions, err)
		return nil, nil
	}

	// Start a server and create a client using advancedtls API with Provider.
	options := &advancedtls.ServerOptions{
		IdentityOptions: advancedtls.IdentityCertificateOptions{
			IdentityProvider: serverIdentityProvider,
		},
		RootOptions: advancedtls.RootCertificateOptions{
			RootProvider: serverRootProvider,
		},
		RequireClientCert: true,
		VType:             advancedtls.CertVerification,
	}
	allowedCommonNames := config.GetString(component + ".allowed_commonNames")
	allowedWildcardDomain := config.GetString("grpc.allowed_wildcard_domain")
	if allowedCommonNames != "" || allowedWildcardDomain != "" {
		allowedCommonNamesMap := make(map[string]bool)
		for _, s := range strings.Split(allowedCommonNames, ",") {
			allowedCommonNamesMap[s] = true
		}
		auther := Authenticator{
			AllowedCommonNames:    allowedCommonNamesMap,
			AllowedWildcardDomain: allowedWildcardDomain,
		}
		options.VerifyPeer = auther.Authenticate
	} else {
		options.VerifyPeer = func(params *advancedtls.VerificationFuncParams) (*advancedtls.VerificationResults, error) {
			return &advancedtls.VerificationResults{}, nil
		}
	}
	ta, err := advancedtls.NewServerCreds(options)
	if err != nil {
		glog.Warningf("advancedtls.NewServerCreds(%v) failed: %v", options, err)
		return nil, nil
	}
	return grpc.Creds(ta), nil
}

func LoadClientTLS(config *util.ViperProxy, component string) grpc.DialOption {
	if config == nil {
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	certFileName, keyFileName, caFileName := config.GetString(component+".cert"), config.GetString(component+".key"), config.GetString("grpc.ca")
	if certFileName == "" || keyFileName == "" || caFileName == "" {
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	clientOptions := pemfile.Options{
		CertFile:        certFileName,
		KeyFile:         keyFileName,
		RefreshDuration: CredRefreshingInterval,
	}
	clientProvider, err := pemfile.NewProvider(clientOptions)
	if err != nil {
		glog.Warningf("pemfile.NewProvider(%v) failed %v", clientOptions, err)
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	clientRootOptions := pemfile.Options{
		RootFile:        config.GetString("grpc.ca"),
		RefreshDuration: CredRefreshingInterval,
	}
	clientRootProvider, err := pemfile.NewProvider(clientRootOptions)
	if err != nil {
		glog.Warningf("pemfile.NewProvider(%v) failed: %v", clientRootOptions, err)
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	options := &advancedtls.ClientOptions{
		IdentityOptions: advancedtls.IdentityCertificateOptions{
			IdentityProvider: clientProvider,
		},
		VerifyPeer: func(params *advancedtls.VerificationFuncParams) (*advancedtls.VerificationResults, error) {
			return &advancedtls.VerificationResults{}, nil
		},
		RootOptions: advancedtls.RootCertificateOptions{
			RootProvider: clientRootProvider,
		},
		VType: advancedtls.CertVerification,
	}
	ta, err := advancedtls.NewClientCreds(options)
	if err != nil {
		glog.Warningf("advancedtls.NewClientCreds(%v) failed: %v", options, err)
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	return grpc.WithTransportCredentials(ta)
}

func LoadCertsFromPEM(path string) (*x509.CertPool, error) {
	clientCerts, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(clientCerts)
	if !ok {
		return nil, fmt.Errorf("error parsing certificate from %s", path)
	}

	return certPool, nil
}

func LoadClientTLSHTTP(clientCertFile string) *tls.Config {
	certPool, err := LoadCertsFromPEM(clientCertFile)
	if err != nil {
		glog.Fatal(fmt.Errorf("error processing client certificate: %v", err))
	}

	return &tls.Config{
		ClientCAs:  certPool,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
}

func (a Authenticator) Authenticate(params *advancedtls.VerificationFuncParams) (*advancedtls.VerificationResults, error) {
	if a.AllowedWildcardDomain != "" && strings.HasSuffix(params.Leaf.Subject.CommonName, a.AllowedWildcardDomain) {
		return &advancedtls.VerificationResults{}, nil
	}
	if _, ok := a.AllowedCommonNames[params.Leaf.Subject.CommonName]; ok {
		return &advancedtls.VerificationResults{}, nil
	}
	err := fmt.Errorf("Authenticate: invalid subject client common name: %s", params.Leaf.Subject.CommonName)
	glog.Error(err)
	return nil, err
}
