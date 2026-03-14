/*
 *
 * Copyright 2024 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package transport_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/http/httpproxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/internal/grpctest"
	"google.golang.org/grpc/internal/resolver/delegatingresolver"
	"google.golang.org/grpc/internal/stubserver"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/internal/testutils/proxyserver"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

const defaultTestTimeout = 10 * time.Second

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

func startBackendServer(t *testing.T) *stubserver.StubServer {
	t.Helper()
	backend := &stubserver.StubServer{
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) { return &testpb.Empty{}, nil },
	}
	if err := backend.StartServer(); err != nil {
		t.Fatalf("failed to start backend: %v", err)
	}
	t.Logf("Started TestService backend at: %q", backend.Address)
	t.Cleanup(backend.Stop)
	return backend
}

func isIPAddr(addr string) bool {
	_, err := netip.ParseAddr(addr)
	return err == nil
}

func overrideTestHTTPSProxy(t *testing.T, proxyAddr string) {
	t.Helper()
	hpfe := func(*http.Request) (*url.URL, error) {
		return &url.URL{
			Scheme: "https",
			Host:   proxyAddr,
		}, nil
	}
	originalhpfe := delegatingresolver.HTTPSProxyFromEnvironment
	delegatingresolver.HTTPSProxyFromEnvironment = hpfe
	t.Cleanup(func() { delegatingresolver.HTTPSProxyFromEnvironment = originalhpfe })
}

// Tests the scenario where grpc.Dial is performed using a proxy with the
// default resolver in the target URI. The test verifies that the connection is
// established to the proxy server, sends the unresolved target URI in the HTTP
// CONNECT request and is successfully connected to the backend server.
func (s) TestGRPCDialWithProxy(t *testing.T) {
	backend := startBackendServer(t)
	unresolvedTargetURI := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, backend.Address))
	proxyCalled := false
	reqCheck := func(req *http.Request) {
		proxyCalled = true
		host, _, err := net.SplitHostPort(req.URL.Host)
		if err != nil {
			t.Error(err)
		}
		if got, want := host, "localhost"; got != want {
			t.Errorf(" Unexpected request host: %s, want = %s ", got, want)
		}
	}
	pServer := proxyserver.New(t, reqCheck, false)
	// Use "localhost:<port>" to verify the proxy address is handled
	// correctly by the delegating resolver and connects to the proxy server
	// correctly even when unresolved.
	pAddr := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, pServer.Addr))

	overrideTestHTTPSProxy(t, pAddr)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	conn, err := grpc.Dial(unresolvedTargetURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.Dial(%s) failed: %v", unresolvedTargetURI, err)
	}
	defer conn.Close()

	// Send an empty RPC to the backend through the proxy.
	client := testgrpc.NewTestServiceClient(conn)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall failed: %v", err)
	}

	if !proxyCalled {
		t.Fatalf("Proxy not connected")
	}
}

// Tests the scenario where `grpc.Dial` is performed with a proxy and the "dns"
// scheme for the target. The test verifies that the proxy URI is correctly
// resolved and that the target URI resolution on the client preserves the
// original behavior of `grpc.Dial`. It also ensures that a connection is
// established to the proxy server, with the resolved target URI sent in the
// HTTP CONNECT request, successfully connecting to the backend server.
func (s) TestGRPCDialWithDNSAndProxy(t *testing.T) {
	backend := startBackendServer(t)
	unresolvedTargetURI := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, backend.Address))
	proxyCalled := false
	reqCheck := func(req *http.Request) {
		proxyCalled = true

		host, _, err := net.SplitHostPort(req.URL.Host)
		if err != nil {
			t.Error(err)
		}
		if got, want := isIPAddr(host), true; got != want {
			t.Errorf("isIPAddr(%q) = %t, want = %t", host, got, want)
		}
	}
	pServer := proxyserver.New(t, reqCheck, false)

	overrideTestHTTPSProxy(t, pServer.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	conn, err := grpc.Dial("dns:///"+unresolvedTargetURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.Dial(%s) failed: %v", "dns:///"+unresolvedTargetURI, err)
	}
	defer conn.Close()

	// Send an empty RPC to the backend through the proxy.
	client := testgrpc.NewTestServiceClient(conn)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall failed: %v", err)
	}

	if !proxyCalled {
		t.Fatalf("Proxy not connected")
	}
}

// Tests the scenario where `grpc.NewClient` is used with the default DNS
// resolver for the target URI and a proxy is configured. The test verifies
// that the client resolves proxy URI, connects to the proxy server, sends the
// unresolved target URI in the HTTP CONNECT request, and successfully
// establishes a connection to the backend server.
func (s) TestNewClientWithProxy(t *testing.T) {
	backend := startBackendServer(t)
	unresolvedTargetURI := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, backend.Address))
	proxyCalled := false
	reqCheck := func(req *http.Request) {
		proxyCalled = true
		host, _, err := net.SplitHostPort(req.URL.Host)
		if err != nil {
			t.Error(err)
		}
		if got, want := host, "localhost"; got != want {
			t.Errorf(" Unexpected request host: %s, want = %s ", got, want)
		}
	}
	pServer := proxyserver.New(t, reqCheck, false)
	// Use "localhost:<port>" to verify the proxy address is handled
	// correctly by the delegating resolver and connects to the proxy server
	// correctly even when unresolved.
	pAddr := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, pServer.Addr))

	overrideTestHTTPSProxy(t, pAddr)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	conn, err := grpc.NewClient(unresolvedTargetURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%s) failed: %v", unresolvedTargetURI, err)
	}
	defer conn.Close()

	// Send an empty RPC to the backend through the proxy.
	client := testgrpc.NewTestServiceClient(conn)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall failed: %v", err)
	}
	if !proxyCalled {
		t.Fatalf("Proxy not connected")
	}
}

// Tests the scenario where grpc.NewClient is used with a custom target URI
// scheme and a proxy is configured. The test verifies that the client
// successfully connects to the proxy server, resolves the proxy URI correctly,
// includes the resolved target URI in the HTTP CONNECT request, and
// establishes a connection to the backend server.
func (s) TestNewClientWithProxyAndCustomResolver(t *testing.T) {
	backend := startBackendServer(t)
	unresolvedTargetURI := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, backend.Address))
	proxyCalled := false
	reqCheck := func(req *http.Request) {
		proxyCalled = true
		host, _, err := net.SplitHostPort(req.URL.Host)
		if err != nil {
			t.Error(err)
		}
		if got, want := isIPAddr(host), true; got != want {
			t.Errorf("isIPAddr(%q) = %t, want = %t", host, got, want)
		}
	}
	pServer := proxyserver.New(t, reqCheck, false)

	overrideTestHTTPSProxy(t, pServer.Addr)

	// Create and update a custom resolver for target URI.
	targetResolver := manual.NewBuilderWithScheme("test")
	resolver.Register(targetResolver)
	targetResolver.InitialState(resolver.State{Endpoints: []resolver.Endpoint{{Addresses: []resolver.Address{{Addr: backend.Address}}}}})

	// Dial to the proxy server.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	conn, err := grpc.NewClient(targetResolver.Scheme()+":///"+unresolvedTargetURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%s) failed: %v", targetResolver.Scheme()+":///"+unresolvedTargetURI, err)
	}
	defer conn.Close()

	// Send an empty RPC to the backend through the proxy.
	client := testgrpc.NewTestServiceClient(conn)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}

	if !proxyCalled {
		t.Fatalf("Proxy not connected")
	}
}

// Tests the scenario where grpc.NewClient is used with the default "dns"
// resolver and the dial option grpc.WithLocalDNSResolution() is set,
// enabling target resolution on the client. The test verifies that target
// resolution happens on the client by sending resolved target URI in HTTP
// CONNECT request, the proxy URI is resolved correctly, and the connection is
// successfully established with the backend server through the proxy.
func (s) TestNewClientWithProxyAndTargetResolutionEnabled(t *testing.T) {
	backend := startBackendServer(t)
	unresolvedTargetURI := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, backend.Address))
	proxyCalled := false
	reqCheck := func(req *http.Request) {
		proxyCalled = true
		host, _, err := net.SplitHostPort(req.URL.Host)
		if err != nil {
			t.Error(err)
		}
		if got, want := isIPAddr(host), true; got != want {
			t.Errorf("isIPAddr(%q) = %t, want = %t", host, got, want)
		}
	}
	pServer := proxyserver.New(t, reqCheck, false)

	overrideTestHTTPSProxy(t, pServer.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	conn, err := grpc.NewClient(unresolvedTargetURI, grpc.WithLocalDNSResolution(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%s) failed: %v", unresolvedTargetURI, err)
	}
	defer conn.Close()

	// Send an empty RPC to the backend through the proxy.
	client := testgrpc.NewTestServiceClient(conn)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall failed: %v", err)
	}

	if !proxyCalled {
		t.Fatalf("Proxy not connected")
	}
}

// Tests the scenario where grpc.NewClient is used with grpc.WithNoProxy() set,
// explicitly disabling proxy usage. The test verifies that the client does not
// dial the proxy but directly connects to the backend server. It also checks
// that the proxy resolution function is not called and that the proxy server
// never receives a connection request.
func (s) TestNewClientWithNoProxy(t *testing.T) {
	backend := startBackendServer(t)
	unresolvedTargetURI := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, backend.Address))
	reqCheck := func(_ *http.Request) { t.Error("proxy server should not have received a Connect request") }
	pServer := proxyserver.New(t, reqCheck, false)

	overrideTestHTTPSProxy(t, pServer.Addr)

	dopts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithNoProxy(), // Disable proxy.
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	conn, err := grpc.NewClient(unresolvedTargetURI, dopts...)
	if err != nil {
		t.Fatalf("grpc.NewClient(%s) failed: %v", unresolvedTargetURI, err)
	}
	defer conn.Close()

	// Create a test service client and make an RPC call.
	client := testgrpc.NewTestServiceClient(conn)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}
}

// Tests the scenario where grpc.NewClient is used with grpc.WithContextDialer()
// set. The test verifies that the client bypasses proxy dialing and uses the
// custom dialer instead. It ensures that the proxy server is never dialed, the
// proxy resolution function is not triggered, and the custom dialer is invoked
// as expected.
func (s) TestNewClientWithContextDialer(t *testing.T) {
	backend := startBackendServer(t)
	unresolvedTargetURI := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, backend.Address))
	reqCheck := func(_ *http.Request) { t.Error("proxy server should not have received a Connect request") }
	pServer := proxyserver.New(t, reqCheck, false)

	overrideTestHTTPSProxy(t, pServer.Addr)

	// Create a custom dialer that directly dials the backend.
	customDialer := func(_ context.Context, unresolvedTargetURI string) (net.Conn, error) {
		return net.Dial("tcp", unresolvedTargetURI)
	}

	dopts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(customDialer), // Use a custom dialer.
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	conn, err := grpc.NewClient(unresolvedTargetURI, dopts...)
	if err != nil {
		t.Fatalf("grpc.NewClient(%s) failed: %v", unresolvedTargetURI, err)
	}
	defer conn.Close()

	client := testgrpc.NewTestServiceClient(conn)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}
}

// Tests the scenario where grpc.NewClient is used with the default DNS resolver
// for targetURI and a proxy. The test verifies that the client connects to the
// proxy server, sends the unresolved target URI in the HTTP CONNECT request,
// and successfully connects to the backend. Additionally, it checks that the
// correct user information is included in the Proxy-Authorization header of
// the CONNECT request. The test also ensures that target resolution does not
// happen on the client.
func (s) TestBasicAuthInNewClientWithProxy(t *testing.T) {
	unresolvedTargetURI := "example.test"
	const (
		user     = "notAUser"
		password = "notAPassword"
	)
	proxyCalled := false
	reqCheck := func(req *http.Request) {
		proxyCalled = true
		if got, want := req.URL.Host, "example.test:443"; got != want {
			t.Errorf(" Unexpected request host: %s, want = %s ", got, want)
		}
		wantProxyAuthStr := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
		if got := req.Header.Get("Proxy-Authorization"); got != wantProxyAuthStr {
			gotDecoded, err := base64.StdEncoding.DecodeString(got)
			if err != nil {
				t.Errorf("failed to decode Proxy-Authorization header: %v", err)
			}
			wantDecoded, _ := base64.StdEncoding.DecodeString(wantProxyAuthStr)
			t.Errorf("unexpected auth %q (%q), want %q (%q)", got, gotDecoded, wantProxyAuthStr, wantDecoded)
		}
	}
	pServer := proxyserver.New(t, reqCheck, false)

	t.Setenv("HTTPS_PROXY", user+":"+password+"@"+pServer.Addr)

	// Use the httpproxy package functions instead of `http.ProxyFromEnvironment`
	// because the latter reads proxy-related environment variables only once at
	// initialization. This behavior causes issues when running test multiple
	// times, as changes to environment variables during tests would be ignored.
	// By using `httpproxy.FromEnvironment()`, we ensure proxy settings are read dynamically.
	origHTTPSProxyFromEnvironment := delegatingresolver.HTTPSProxyFromEnvironment
	delegatingresolver.HTTPSProxyFromEnvironment = func(req *http.Request) (*url.URL, error) {
		return httpproxy.FromEnvironment().ProxyFunc()(req.URL)
	}
	defer func() {
		delegatingresolver.HTTPSProxyFromEnvironment = origHTTPSProxyFromEnvironment
	}()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	conn, err := grpc.NewClient(unresolvedTargetURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%s) failed: %v", unresolvedTargetURI, err)
	}
	defer conn.Close()

	// Send an empty RPC to the backend through the proxy.
	client := testgrpc.NewTestServiceClient(conn)
	client.EmptyCall(ctx, &testpb.Empty{})

	if !proxyCalled {
		t.Fatalf("Proxy not connected")
	}
}

// Tests the scenario where grpc.NewClient is used with an HTTPS proxy (proxy
// server itself requires TLS). The test verifies that the client establishes a
// TLS connection to the proxy, performs the HTTP CONNECT handshake over TLS,
// and successfully connects to the backend server. The ROOT_CA_CERT
// environment variable is used to specify the CA certificate for verifying
// the proxy's TLS certificate.
func (s) TestNewClientWithHTTPSProxy(t *testing.T) {
	backend := startBackendServer(t)
	unresolvedTargetURI := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, backend.Address))
	proxyCalled := false
	reqCheck := func(req *http.Request) {
		proxyCalled = true
		host, _, err := net.SplitHostPort(req.URL.Host)
		if err != nil {
			t.Error(err)
		}
		if got, want := host, "localhost"; got != want {
			t.Errorf("Unexpected request host: %s, want = %s", got, want)
		}
	}
	pServer, caCertPEM := proxyserver.NewTLS(t, reqCheck, false)

	// Write the CA cert to a temp file for ROOT_CA_CERT.
	caFile := filepath.Join(t.TempDir(), "proxy-ca.crt")
	if err := os.WriteFile(caFile, caCertPEM, 0600); err != nil {
		t.Fatalf("failed to write CA cert file: %v", err)
	}
	t.Setenv("ROOT_CA_CERT", caFile)

	pAddr := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, pServer.Addr))

	// Override proxy resolution to return an https scheme URL.
	orighpfe := delegatingresolver.HTTPSProxyFromEnvironment
	delegatingresolver.HTTPSProxyFromEnvironment = func(req *http.Request) (*url.URL, error) {
		if req.URL.Host == unresolvedTargetURI {
			return &url.URL{
				Scheme: "https",
				Host:   pAddr,
			}, nil
		}
		t.Errorf("Unexpected request host to proxy: %s want %s", req.URL.Host, unresolvedTargetURI)
		return nil, nil
	}
	defer func() { delegatingresolver.HTTPSProxyFromEnvironment = orighpfe }()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	conn, err := grpc.NewClient(unresolvedTargetURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%s) failed: %v", unresolvedTargetURI, err)
	}
	defer conn.Close()

	// Send an empty RPC to the backend through the HTTPS proxy.
	client2 := testgrpc.NewTestServiceClient(conn)
	if _, err := client2.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall failed: %v", err)
	}

	if !proxyCalled {
		t.Fatalf("HTTPS Proxy not connected")
	}
}

// Tests the scenario where grpc.NewClient is used with an HTTPS proxy that
// has a self-signed certificate, but no ROOT_CA_CERT is set. The TLS
// handshake to the proxy should fail because the proxy's cert is not trusted
// by the system certificate pool.
func (s) TestNewClientWithHTTPSProxyNoCACert(t *testing.T) {
	backend := startBackendServer(t)
	unresolvedTargetURI := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, backend.Address))

	// Create a minimal TLS listener with a self-signed certificate. We don't
	// use proxyserver.NewTLS here because its handleRequest calls t.Errorf
	// when it fails to read the CONNECT request, which is the expected
	// behavior in this negative test (TLS handshake fails).
	pLis, err := testutils.LocalTCPListener()
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer pLis.Close()
	key, kerr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if kerr != nil {
		t.Fatalf("failed to generate key: %v", kerr)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
	}
	certDER, cerr := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if cerr != nil {
		t.Fatalf("failed to create cert: %v", cerr)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, merr := x509.MarshalECPrivateKey(key)
	if merr != nil {
		t.Fatalf("failed to marshal key: %v", merr)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	tlsCert, terr := tls.X509KeyPair(certPEM, keyPEM)
	if terr != nil {
		t.Fatalf("failed to load key pair: %v", terr)
	}

	tlsLis := tls.NewListener(pLis, &tls.Config{Certificates: []tls.Certificate{tlsCert}})
	go func() {
		for {
			conn, err := tlsLis.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	t.Cleanup(func() { tlsLis.Close() })

	pAddr := fmt.Sprintf("localhost:%d", testutils.ParsePort(t, pLis.Addr().String()))

	// Ensure ROOT_CA_CERT is not set; system CAs won't trust self-signed cert.
	t.Setenv("ROOT_CA_CERT", "")

	// Override proxy resolution to return an https scheme URL.
	orighpfe := delegatingresolver.HTTPSProxyFromEnvironment
	delegatingresolver.HTTPSProxyFromEnvironment = func(req *http.Request) (*url.URL, error) {
		if req.URL.Host == unresolvedTargetURI {
			return &url.URL{
				Scheme: "https",
				Host:   pAddr,
			}, nil
		}
		return nil, nil
	}
	defer func() { delegatingresolver.HTTPSProxyFromEnvironment = orighpfe }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(unresolvedTargetURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%s) failed: %v", unresolvedTargetURI, err)
	}
	defer conn.Close()

	// The RPC should fail because the TLS handshake to the proxy fails
	// (self-signed cert not trusted by system CAs).
	client2 := testgrpc.NewTestServiceClient(conn)
	_, err = client2.EmptyCall(ctx, &testpb.Empty{})
	if err == nil {
		t.Fatal("EmptyCall should have failed due to TLS verification error, but succeeded")
	}
	t.Logf("Expected TLS error: %v", err)
}
