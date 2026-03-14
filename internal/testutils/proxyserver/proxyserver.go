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

// Package proxyserver provides an implementation of a proxy server for testing purposes.
// The server supports only a single incoming connection at a time and is not concurrent.
// It handles only HTTP CONNECT requests; other HTTP methods are not supported.
package proxyserver

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc/internal/testutils"
)

// ProxyServer represents a test proxy server.
type ProxyServer struct {
	lis       net.Listener
	in        net.Conn            // Connection from the client to the proxy.
	out       net.Conn            // Connection from the proxy to the backend.
	onRequest func(*http.Request) // Function to check the request sent to proxy.
	Addr      string              // Address of the proxy
}

const defaultTestTimeout = 10 * time.Second

// Stop closes the ProxyServer and its connections to client and server.
func (p *ProxyServer) stop() {
	p.lis.Close()
	if p.in != nil {
		p.in.Close()
	}
	if p.out != nil {
		p.out.Close()
	}
}

func (p *ProxyServer) handleRequest(t *testing.T, in net.Conn, waitForServerHello bool) {
	req, err := http.ReadRequest(bufio.NewReader(in))
	if err != nil {
		t.Errorf("failed to read CONNECT req: %v", err)
		return
	}
	if req.Method != http.MethodConnect {
		t.Errorf("unexpected Method %q, want %q", req.Method, http.MethodConnect)
	}
	p.onRequest(req)

	t.Logf("Dialing to %s", req.URL.Host)
	out, err := net.Dial("tcp", req.URL.Host)
	if err != nil {
		in.Close()
		t.Logf("failed to dial to server: %v", err)
		return
	}
	out.SetDeadline(time.Now().Add(defaultTestTimeout))
	resp := http.Response{StatusCode: http.StatusOK, Proto: "HTTP/1.0"}
	var buf bytes.Buffer
	resp.Write(&buf)

	if waitForServerHello {
		// Batch the first message from the server with the http connect
		// response. This is done to test the cases in which the grpc client has
		// the response to the connect request and proxied packets from the
		// destination server when it reads the transport.
		b := make([]byte, 50)
		bytesRead, err := out.Read(b)
		if err != nil {
			t.Errorf("Got error while reading server hello: %v", err)
			in.Close()
			out.Close()
			return
		}
		buf.Write(b[0:bytesRead])
	}
	p.in = in
	p.in.Write(buf.Bytes())
	p.out = out

	go io.Copy(p.in, p.out)
	go io.Copy(p.out, p.in)
}

// New initializes and starts a proxy server, registers a cleanup to
// stop it, and returns a ProxyServer.
func New(t *testing.T, reqCheck func(*http.Request), waitForServerHello bool) *ProxyServer {
	t.Helper()
	pLis, err := testutils.LocalTCPListener()
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	p := &ProxyServer{
		lis:       pLis,
		onRequest: reqCheck,
		Addr:      pLis.Addr().String(),
	}

	// Start the proxy server.
	go func() {
		for {
			in, err := p.lis.Accept()
			if err != nil {
				return
			}
			// p.handleRequest is not invoked in a goroutine because the test
			// proxy currently supports handling only one connection at a time.
			p.handleRequest(t, in, waitForServerHello)
		}
	}()
	t.Logf("Started proxy at: %q", pLis.Addr().String())
	t.Cleanup(p.stop)
	return p
}

// NewTLS initializes and starts a TLS-enabled proxy server. It generates a
// self-signed certificate for the proxy and returns the ProxyServer along
// with the PEM-encoded CA certificate bytes that clients should use to
// verify the proxy's TLS certificate.
func NewTLS(t *testing.T, reqCheck func(*http.Request), waitForServerHello bool) (*ProxyServer, []byte) {
	t.Helper()
	pLis, err := testutils.LocalTCPListener()
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	// Generate a self-signed CA and server certificate for the proxy.
	tlsCert, caCertPEM := generateSelfSignedCert(t)
	tlsLis := tls.NewListener(pLis, &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	})

	p := &ProxyServer{
		lis:       tlsLis,
		onRequest: reqCheck,
		Addr:      pLis.Addr().String(),
	}

	go func() {
		for {
			in, err := p.lis.Accept()
			if err != nil {
				return
			}
			p.handleRequest(t, in, waitForServerHello)
		}
	}()
	t.Logf("Started TLS proxy at: %q", pLis.Addr().String())
	t.Cleanup(p.stop)
	return p, caCertPEM
}

// generateSelfSignedCert creates a self-signed TLS certificate for testing.
// Returns the tls.Certificate and the PEM-encoded CA certificate bytes.
func generateSelfSignedCert(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-proxy"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
		IsCA:         true,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("failed to marshal private key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("failed to load key pair: %v", err)
	}
	return tlsCert, certPEM
}
