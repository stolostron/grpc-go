/*
 *
 * Copyright 2017 gRPC authors.
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

package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"

	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/internal/proxyattributes"
	"google.golang.org/grpc/resolver"
)

const proxyAuthHeaderKey = "Proxy-Authorization"

// proxyCAEnvVar is an environment variable to specify the CA certificate file
// for verifying TLS connections to HTTPS proxy servers. If not set, the system
// certificate pool is used.
const proxyCAEnvVar = "ROOT_CA_CERT"

// To read a response from a net.Conn, http.ReadResponse() takes a bufio.Reader.
// It's possible that this reader reads more than what's need for the response
// and stores those bytes in the buffer. bufConn wraps the original net.Conn
// and the bufio.Reader to make sure we don't lose the bytes in the buffer.
type bufConn struct {
	net.Conn
	r io.Reader
}

func (c *bufConn) Read(b []byte) (int, error) {
	return c.r.Read(b)
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func doHTTPConnectHandshake(ctx context.Context, conn net.Conn, grpcUA string, opts proxyattributes.Options) (_ net.Conn, err error) {
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: opts.ConnectAddr},
		Header: map[string][]string{"User-Agent": {grpcUA}},
	}
	if user := opts.User; user != nil {
		u := user.Username()
		p, _ := user.Password()
		req.Header.Add(proxyAuthHeaderKey, "Basic "+basicAuth(u, p))
	}
	if err := sendHTTPRequest(ctx, req, conn); err != nil {
		return nil, fmt.Errorf("failed to write the HTTP request: %v", err)
	}

	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, req)
	if err != nil {
		return nil, fmt.Errorf("reading server HTTP response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		dump, err := httputil.DumpResponse(resp, true)
		if err != nil {
			return nil, fmt.Errorf("failed to do connect handshake, status code: %s", resp.Status)
		}
		return nil, fmt.Errorf("failed to do connect handshake, response: %q", dump)
	}
	// The buffer could contain extra bytes from the target server, so we can't
	// discard it. However, in many cases where the server waits for the client
	// to send the first message (e.g. when TLS is being used), the buffer will
	// be empty, so we can avoid the overhead of reading through this buffer.
	if r.Buffered() != 0 {
		return &bufConn{Conn: conn, r: r}, nil
	}
	return conn, nil
}

// proxyDial establishes a TCP connection to the specified address and performs
// an HTTP CONNECT handshake. When the proxy scheme is "https", a TLS
// connection is established to the proxy server first. The CA certificate for
// verifying the proxy's TLS certificate can be specified via the
// ROOT_CA_CERT environment variable; if not set, the system certificate
// pool is used.
func proxyDial(ctx context.Context, addr resolver.Address, grpcUA string, opts proxyattributes.Options) (net.Conn, error) {
	conn, err := internal.NetDialerWithTCPKeepalive().DialContext(ctx, "tcp", addr.Addr)
	if err != nil {
		return nil, err
	}

	if opts.ProxyScheme == "https" {
		tlsConf, err := proxyTLSConfig(addr.Addr)
		if err != nil {
			conn.Close()
			return nil, err
		}
		tlsConn := tls.Client(conn, tlsConf)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, fmt.Errorf("TLS handshake to proxy %s failed: %v", addr.Addr, err)
		}
		conn = tlsConn
	}

	return doHTTPConnectHandshake(ctx, conn, grpcUA, opts)
}

// proxyTLSConfig returns a TLS config for connecting to an HTTPS proxy at the
// given address. If ROOT_CA_CERT is set, its CA is used for
// verification. Otherwise, the system certificate pool is used (Go's default
// behavior when RootCAs is nil).
func proxyTLSConfig(proxyAddr string) (*tls.Config, error) {
	// Extract the hostname from the proxy address for SNI/verification.
	host, _, err := net.SplitHostPort(proxyAddr)
	if err != nil {
		host = proxyAddr
	}

	caCertPool, err := loadProxyCACertPool()
	if err != nil {
		return nil, err
	}
	// When caCertPool is nil, Go uses the system certificate pool by default.
	return &tls.Config{ServerName: host, RootCAs: caCertPool}, nil
}

func sendHTTPRequest(ctx context.Context, req *http.Request, conn net.Conn) error {
	req = req.WithContext(ctx)
	if err := req.Write(conn); err != nil {
		return fmt.Errorf("failed to write the HTTP request: %v", err)
	}
	return nil
}

// loadProxyCACertPool loads the CA certificate specified by the ROOT_CA_CERT
// environment variable into a certificate pool. Returns (nil, nil) when the
// environment variable is not set.
func loadProxyCACertPool() (*x509.CertPool, error) {
	caFile := os.Getenv(proxyCAEnvVar)
	if caFile == "" {
		return nil, nil
	}
	certPool := x509.NewCertPool()
	caCert, err := os.ReadFile(filepath.Clean(caFile))
	if err != nil {
		return nil, fmt.Errorf("failed to read proxy CA cert %s: %v", caFile, err)
	}
	if ok := certPool.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("failed to append proxy CA cert to the cert pool")
	}
	return certPool, nil
}
