# Port proxy: acceptance criteria for forwarding

Status: `PP-1` met by #277, which fixed the second bug a property test found against it (#245 fixed
the first). `PP-2` met by #316, forwarding the raw query and bare `?` unchanged. Each criterion
names the tests that check it on every pull request (the Go job's `go test -race ./...`).

## Scope

ate-env-api's port proxy
([internal/portproxy/portproxy.go](../../../internal/portproxy/portproxy.go)) forwards an HTTP or
WebSocket request to a port a daemon listens on inside an environment. A caller addresses the
environment and port in one of two forms:

- **Header form:** `X-Env-Id`, `X-Env-Port` (and `X-Env-Atespace`) headers. The proxy forwards the
  request's own path and query.
- **Path form:** `/v1alpha/envs/<id>/ports/<port>/<rest>` (or
  `/v1alpha/atespaces/<atespace>/envs/<id>/ports/<port>/<rest>`), for browsers and WebSocket clients
  that cannot set headers. The proxy forwards `/<rest>` and the query.

`Dispatch` matches the path form itself rather than on a `ServeMux` "so the daemon gets the path
exactly as sent": a mux would redirect a missing trailing slash, `//` and `..`, and WebSocket
clients do not follow redirects.

`PP-1` has one exception: a forwarded path that starts with `//` would read as an authority if
written as is, so in it the proxy percent-encodes the bytes `URL.RawPath` cannot hold raw
(`{ } | ^ " < > \`, `` ` ``, `#`, space, control bytes and non-ASCII) and keeps every other byte,
escapes included. The daemon gets the same segments, each decoding to the same bytes, and gets the
query byte for byte (`PP-2`).

## Criteria

| ID   | Criterion                                                                                                                                                                                                                                 | Status      | Tests                                                                                                                                                                       |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PP-1 | The daemon gets the forwarded path exactly as the caller sent it, byte for byte, in both forms: the path form's `/<rest>` after the routing prefix, and the header form's path. No character is escaped, unescaped or cleaned on the way. | met by #277 | `TestPathFormDeliversPathAsSentProperty`, `TestPathFormKeepsSubDelimsAsSent`, `TestPathFormKeepsPathAsSent`, `TestRawBytesKeptAsSent`, `TestRawBytesDoNotMoveRoutingPrefix` |
| PP-2 | The daemon gets the query of the request-target (including a bare trailing `?`, `;` parameters, bad `%` escapes, `{ } \| ^`, `#`, and raw UTF-8) exactly as the caller sent it, byte for byte, in both forms, including on `//` paths.    | met by #316 | `TestPathFormDeliversPathAsSentProperty`, `TestRawBytesKeptAsSent`, `TestRawQueryKeptAsSent`                                                                                |

## What the property test found

- **PP-1.** When the path form's rest held no percent-escape, `parsePath` dropped the raw path (it
  equalled the decoded one), and Go's `url.EscapedPath` re-escaped the characters it escapes in a
  path though they are valid as sent: `/!` reached the daemon as `/%21` and `/files/report(1).txt`
  as `/files/report%281%29.txt`. The header form delivered them unchanged. Found by
  `TestPathFormDeliversPathAsSentProperty`, which generates paths from unreserved characters,
  sub-delims, `:` `@` and percent-escapes and sends each through both forms; the example tests used
  unreserved characters and escapes only, and the bug needs a sub-delim with no escape anywhere in
  the rest.
- **PP-1, again.** Go's server accepts bytes in a request-target that `url.EscapedPath` calls
  invalid in a raw path (`{ } | ^ " < > \`, `` ` ``, `#` and raw UTF-8). The server keeps them in
  `URL.RawPath`, but `EscapedPath` drops such a `RawPath` and re-escapes the decoded path, in both
  forms: `/a{b}` reached the daemon as `/a%7Bb%7D`, and `/a{b%2Fc` as `/a%7Bb/c`, with one segment
  more. The path form also matched its routing prefix on that re-escaped path, so an escaped slash
  routed: `/v1alpha%2Fenvs%2Fenv1%2Fports%2F8000/a{` reached the daemon. The proxy now reads the
  path as sent from `URL.RawPath`, matches the prefix on it, and writes it to the daemon as the
  request-target. A path that starts with `//` would read as an authority if written as is, so in it
  only those bytes are escaped and the segments stay. Found by an audit of the criterion: the
  property test sent its paths through `http.Client`, which escapes these bytes itself, and
  generated none of them. It now writes each request line on a raw TCP connection, generates these
  bytes mixed with `%2F`, a leading `//` and a query, and also sends the routing prefix with one
  separator escaped, which must reach no daemon. On the old code it fails at once, shrunk to `/{`.
- **PP-2.** `httputil.ReverseProxy` runs `cleanQueryParams` on the outbound `URL.RawQuery` before
  calling `Rewrite`. When a query holds `;` or a bad `%` escape, `cleanQueryParams` re-encodes it
  through `url.ParseQuery`, which drops parameters with `;` or bad escapes and sorts and re-encodes
  the rest: `/?;` reached the daemon as `/`, `/x?a=1;b=2` as `/x`, and `/?z=1&a=2%203&semi;drop` as
  `/?a=2+3&z=1`. The proxy never inspects the query, so `rewrite` copies `RawQuery` from
  `pr.In.URL`; `ReverseProxy` keeps `ForceQuery` (a bare `?`) itself.
  `TestPathFormDeliversPathAsSentProperty` now draws bare `?`, `;`, `&`, `/`, `?`, bad `%` escapes,
  `{ } | ^`, `#`, and raw UTF-8 in queries on ordinary and `//` paths (and after a bare path-form
  prefix), and `TestRawQueryKeptAsSent` checks the examples across both forms. On the previous code
  `rapid` shrinks the failure to `/?;`.

It is not a timing bug, so `testing/synctest` alone would not have found it: synctest makes
scheduling deterministic but generates no inputs and checks nothing.
