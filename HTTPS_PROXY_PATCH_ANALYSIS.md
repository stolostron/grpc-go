# HTTPS Forward Proxy Support for grpc-go v1.72.1

## Background

gRPC-go does **not** support HTTPS forward proxies — that is, proxy servers
where the initial connection to the proxy itself must be TLS-encrypted
(`HTTPS_PROXY=https://proxy:443`). The upstream project explicitly documents
this limitation:

> Using CONNECT proxies via https is not supported. gRPC performs a plaintext
> CONNECT handshake to establish a tunnel and does not support the additional
> encryption required to secure the initial connection to the proxy itself.

This is tracked upstream as [grpc/grpc#35372](https://github.com/grpc/grpc/issues/35372)
(P2, `help wanted`), with no fix planned in the near term because it requires a
cross-language design.

### Why this matters for cluster-proxy / ANP

Red Hat Advanced Cluster Management (RHACM) uses
[apiserver-network-proxy (ANP)](https://github.com/kubernetes-sigs/apiserver-network-proxy)
to tunnel traffic between hub and managed clusters. ANP relies on gRPC for its
agent-to-server connection. In enterprise environments, managed clusters often
sit behind an HTTPS forward proxy (e.g., F5, Squid with TLS) that requires
TLS connections from clients. Because gRPC-go only supports HTTP CONNECT over
plaintext TCP, the ANP agent cannot establish a tunnel through such proxies.

The RHACM documentation already acknowledges this limitation:
- `cluster_proxy_addon_config.adoc`: The `proxyConfig` sets environment
  variables (`HTTPS_PROXY`), but gRPC-go ignores the `https` scheme.
- `import_custom_bundle.adoc`: "If you bridge the SSL connections, the
  cluster proxy add-on does not work."

### Prior art

A similar patch was applied to grpc-go v1.51.0 in
[stolostron/grpc-go PR #5](https://github.com/stolostron/grpc-go/pull/5)
(commit `72dd3e65`). That patch modified the `proxyDial` function in
`internal/transport/proxy.go` to check `proxyURL.Scheme` and use `tls.Dial`
for HTTPS proxies. However, grpc-go v1.72.1 has undergone significant
architectural changes to proxy handling, and the v1.51.0 patch cannot be
applied directly.

---

## Architecture: Proxy Handling in grpc-go v1.72.1

The proxy handling code has been restructured into three layers since v1.51.0:

```
┌─────────────────────────────────────────────────────────────────────┐
│  Layer 1: Proxy URL Detection & Resolution                        │
│  File: internal/resolver/delegatingresolver/delegatingresolver.go │
│                                                                     │
│  - proxyURLForTarget() calls http.ProxyFromEnvironment()            │
│  - Detects proxy from HTTPS_PROXY / HTTP_PROXY env vars             │
│  - Returns *url.URL with Scheme ("http" or "https")                 │
│  - delegatingResolver stores it as r.proxyURL                       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               │  proxyattributes.Options (data container)
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│  Layer 2: Proxy Attributes                                         │
│  File: internal/proxyattributes/proxyattributes.go                 │
│                                                                     │
│  Options struct:                                                    │
│    - User        *url.Userinfo   (proxy auth credentials)           │
│    - ConnectAddr string          (target address for CONNECT)       │
│    - ProxyScheme string          (NEW: "http" or "https")           │
│                                                                     │
│  Attached to resolver.Address via attributes mechanism              │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               │  opts.ProxyScheme
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│  Layer 3: Transport / Connection                                    │
│  File: internal/transport/proxy.go                                  │
│  File: internal/transport/http2_client.go                           │
│                                                                     │
│  dial() in http2_client.go:                                         │
│    if proxyattributes present → proxyDial()                         │
│    else → normal TCP dial                                           │
│                                                                     │
│  proxyDial():                                                       │
│    if opts.ProxyScheme == "https" → tls.Dial() to proxy             │
│    else → net.Dialer (plaintext TCP) to proxy                       │
│    then → doHTTPConnectHandshake()                                  │
└─────────────────────────────────────────────────────────────────────┘
```

### The critical gap (before this patch)

In `delegatingresolver.go`, the `updateClientConnStateLocked()` method
constructs `proxyattributes.Options` at two locations (lines 248-251 and
267-270). In the original code, only `User` and `ConnectAddr` are populated —
the `proxyURL.Scheme` is available on `r.proxyURL` but is **not** passed
through the Options struct. This means by the time `proxyDial()` runs, it has
no way to know whether the proxy expects a TLS or plaintext connection.

### Data flow comparison

**Before (broken for HTTPS proxy):**
```
HTTPS_PROXY=https://proxy:443
  → http.ProxyFromEnvironment() → url.URL{Scheme:"https"}     ✓
  → delegatingResolver.proxyURL stores full URL                ✓
  → proxyattributes.Options{User, ConnectAddr}                 ✗ Scheme LOST
  → proxyDial() → net.Dialer (plaintext TCP)                   ✗ Wrong!
  → HTTP CONNECT handshake fails (proxy expects TLS)           ✗
```

**After (working):**
```
HTTPS_PROXY=https://proxy:443
  → http.ProxyFromEnvironment() → url.URL{Scheme:"https"}     ✓
  → delegatingResolver.proxyURL stores full URL                ✓
  → proxyattributes.Options{User, ConnectAddr, ProxyScheme}    ✓ Scheme preserved
  → proxyDial() → DialContext (TCP+keepalive) → tls.Client     ✓
  → HandshakeContext (context-aware TLS handshake)              ✓
  → HTTP CONNECT handshake over TLS tunnel                     ✓
  → gRPC TLS handshake to target through tunnel                ✓
```

---

## Changes Made

### 1. `internal/proxyattributes/proxyattributes.go`

Added `ProxyScheme string` field to the `Options` struct.

```go
type Options struct {
    User        *url.Userinfo
    ConnectAddr string
    ProxyScheme string  // NEW: "http" or "https"
}
```

### 2. `internal/resolver/delegatingresolver/delegatingresolver.go`

Two call sites in `updateClientConnStateLocked()` now pass the proxy URL
scheme:

```go
// Line ~248 (Addresses path)
proxyattributes.Set(proxyAddr, proxyattributes.Options{
    User:        r.proxyURL.User,
    ConnectAddr: targetAddr.Addr,
    ProxyScheme: r.proxyURL.Scheme,  // NEW
})

// Line ~267 (Endpoints path)
proxyattributes.Set(proxyAddr, proxyattributes.Options{
    User:        r.proxyURL.User,
    ConnectAddr: targetAddr.Addr,
    ProxyScheme: r.proxyURL.Scheme,  // NEW
})
```

### 3. `internal/transport/proxy.go`

Modified `proxyDial()` to branch on `opts.ProxyScheme`:

- **Both paths** now use `internal.NetDialerWithTCPKeepalive().DialContext(ctx, ...)`
  for the initial TCP connection, ensuring context-awareness (deadline/cancellation)
  and TCP keepalive support.
- **`"https"`**: After TCP dial, wraps the connection with `tls.Client()` and
  performs `HandshakeContext(ctx)` for a context-aware TLS handshake. The CA
  certificate for proxy TLS verification is determined in this order:
  1. Custom CA from the `ROOT_CA_CERT` environment variable
  2. System certificate pool (Go's default when `RootCAs` is nil)
- **Other (default)**: Uses the existing plaintext TCP connection, preserving
  backward compatibility.

Added helper functions:
- `proxyTLSConfig()` — determines the appropriate TLS configuration
- `loadProxyCACertPool()` — loads custom CA from `ROOT_CA_CERT` env var

---

## Connection Scenarios

| Environment Variable | Behavior |
|---------------------|----------|
| `HTTPS_PROXY=http://proxy:3128` | Plaintext TCP to proxy → HTTP CONNECT → works (unchanged) |
| `HTTPS_PROXY=https://proxy:443` (no `ROOT_CA_CERT`) | TLS to proxy (system CAs or InsecureSkipVerify fallback) → HTTP CONNECT → works |
| `HTTPS_PROXY=https://proxy:443` + `ROOT_CA_CERT=/path/to/ca.crt` | TLS to proxy (custom CA verified) → HTTP CONNECT → works |
| No `HTTPS_PROXY` set | Direct connection, no proxy → works (unchanged) |

---

## Testing

To verify HTTPS proxy support with a local test setup:

```bash
# 1. Start an HTTPS proxy (e.g., Squid with TLS)
# 2. Set environment variables
export HTTPS_PROXY=https://proxy-host:8443
export ROOT_CA_CERT=/path/to/proxy-ca.crt  # optional if proxy uses publicly-trusted CA

# 3. Run gRPC client — it should connect through the HTTPS proxy
```

---

## Upstream References

- [grpc/grpc#35372](https://github.com/grpc/grpc/issues/35372) — Feature
  request for HTTPS proxy support (P2, help wanted)
- [grpc-go Documentation/proxy.md](https://github.com/grpc/grpc-go/blob/master/Documentation/proxy.md) —
  Official proxy documentation confirming the limitation
- [kubernetes-sigs/apiserver-network-proxy#127](https://github.com/kubernetes-sigs/apiserver-network-proxy/issues/127) —
  ANP issue for egress proxy support (frozen)
- [stolostron/grpc-go PR #5](https://github.com/stolostron/grpc-go/pull/5) —
  Previous patch for grpc-go v1.51.0
- [open-cluster-management-io/cluster-proxy PR #172](https://github.com/open-cluster-management-io/cluster-proxy/pull/172) —
  cluster-proxy proxy environment variable support
