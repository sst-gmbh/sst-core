// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package testutil

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/semanticstep/sst-core/defaultderive"
	"github.com/semanticstep/sst-core/sst"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

var errFailedToCreateClientCertPool = errors.New("failed to create client certificate pool")

var (
	testCertsOnce  sync.Once
	testCaCert     []byte
	testServerCert []byte
	testServerKey  []byte
	testCertsErr   error
)

func initTestCerts() {
	testCaCert, testServerCert, testServerKey, testCertsErr = generateTestCerts()
}

func ensureTestCerts() error {
	testCertsOnce.Do(initTestCerts)
	return testCertsErr
}

func generateTestCerts() (caCertPEM []byte, serverCertPEM []byte, serverKeyPEM []byte, err error) {
	// Generate CA key
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, err
	}

	// Generate CA certificate
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, err
	}
	caCertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Generate server key
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, err
	}

	// Generate server certificate signed by CA
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, err
	}
	serverCertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	serverKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})

	return caCertPEM, serverCertPEM, serverKeyPEM, nil
}

// tls.Certificate structure includes the certificate and the private key.
// This structure can then be used to configure a TLS listener or server.
func TestServerCert() (tls.Certificate, error) {
	if err := ensureTestCerts(); err != nil {
		return tls.Certificate{}, err
	}
	cert, err := tls.X509KeyPair(testServerCert, testServerKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	return cert, nil
}

// TestTransportCreds function creates a gRPC DialOption that specifies the transport credentials
// for secure communication using TLS. This is useful when you need to establish a secure gRPC connection to a server.
func TestTransportCreds() (grpc.DialOption, error) {
	if err := ensureTestCerts(); err != nil {
		return nil, err
	}
	clientCertPool := x509.NewCertPool()
	if !clientCertPool.AppendCertsFromPEM(testCaCert) {
		return nil, errFailedToCreateClientCertPool
	}
	return grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(clientCertPool, "")), nil
}

// The function's primary purpose is to create a grpc.DialOption that configures the gRPC client to use OAuth2 credentials for per-RPC authentication.
// This means that every RPC call made by the client will include the OAuth2 access token, allowing the server to verify the client's identity
// and grant access to the requested resources based on the token's validity and permissions.
func TestDialPerRPCOauthCreds(testToken string) grpc.DialOption {
	perRPCCreds := oauth.NewOauthAccess(&oauth2.Token{
		AccessToken: testToken,
	})
	return grpc.WithPerRPCCredentials(perRPCCreds)
}

func TestCallPerRPCOauthCreds(testToken string) grpc.CallOption {
	creds := oauth.NewOauthAccess(&oauth2.Token{
		AccessToken: testToken,
	})
	return grpc.PerRPCCredentials(creds)
}

// for test cases
func ServerServe(t testing.TB, path string) string {
	cert, err := TestServerCert()
	require.NoError(t, err)

	server, err := sst.NewServer(&sst.RepositoryServerConfig{
		RepoDir:    path,
		Issuer:     "test://issuer",
		ServerCert: &cert,
		Verbose:    true,
		DeriveInfo: defaultderive.DeriveInfo(),
	})
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := lis.Addr().(*net.TCPAddr).Port
	url := fmt.Sprintf("localhost:%d", port)

	go func() {
		assert.NoError(t, server.Serve(lis))
	}()
	t.Cleanup(func() {
		require.NoError(t, server.GracefulStopAndClose())
	})

	return url
}

func SuperServerServe(t testing.TB, path string) string {
	cert, err := TestServerCert()
	require.NoError(t, err)

	server, err := sst.NewSuperServer(&sst.RepositoryServerConfig{
		RepoDir:    path,
		Issuer:     "test://issuer",
		ServerCert: &cert,
		Verbose:    true,
		DeriveInfo: defaultderive.DeriveInfo(),
		ClientID:   "test-client-id",
	})
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	port := lis.Addr().(*net.TCPAddr).Port
	url := fmt.Sprintf("localhost:%d", port)

	go func() {
		assert.NoError(t, server.Serve(lis))
	}()
	t.Cleanup(func() {
		require.NoError(t, server.GracefulStopAndClose())
	})

	return url
}

type TestProvider struct {
	RawToken string
	info     func() (email string, name string, err error)
}

func (p TestProvider) AuthProvider()                                {}
func (p TestProvider) Info() (email string, name string, err error) { return p.info() }
func (p TestProvider) Oauth2Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: p.RawToken}, nil
}

var TestProviderInstance = TestProvider{
	RawToken: "test-token-1",
}

var TestProviderInstance2 = TestProvider{
	RawToken: "test-token-2",
}
