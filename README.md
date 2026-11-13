# jsonrpc4k

Kotlin Multiplatform JSON-RPC 2.0 for language servers and similar tools: a transport-independent
connection engine, the LSP base protocol framing (`Content-Length` headers), and byte-transparent
transports for the JVM, Node.js and Kotlin/Native.

> [!NOTE]
> The library is pre-1.0. Breaking changes land without deprecation shims.

## Modules

| Module | Contents |
|---|---|
| `jsonrpc4k-core` | Messages, codec, `Connection` (requests, notifications, batches, cancellation), dispatch and resource limits. No I/O. |
| `jsonrpc4k-transport` | The `Transport` byte-pipe interface and the stdio, TCP, Unix socket, named pipe, Node IPC, child-process and in-memory transports. Depends on core. |

## Protocol coverage

- JSON-RPC 2.0: requests, notifications, responses, error objects, batches (a batch is answered with one
  array frame), Parse error and Invalid Request replies with a null id.
- LSP base protocol: `Content-Length` framing on every byte transport. The Node IPC transport is
  message based and uses no framing.
- `$/cancelRequest` in both directions (see [Cancellation](#cancellation)).
- `LspLaunchArguments.parse` reads the vscode-languageclient flags `--stdio`, `--pipe=`, `--socket=`,
  `--node-ipc` and `--clientProcessId=`.

## Transports by platform

| Transport | JVM | Node.js | Linux and Apple native | Windows native (mingwX64) | Browser |
|---|---|---|---|---|---|
| Stdio | yes | implemented, not executed in CI | implemented, not executed in CI | implemented, not executed in CI | unsupported: no stdin or stdout |
| TCP client (`TcpSocket`) | yes | yes | yes | yes | unsupported: no raw sockets |
| TCP listener (`TcpListener`) | yes | yes | yes | yes | unsupported: cannot accept connections |
| Unix socket (`UnixSocket`) | yes (JDK 16+) | yes, not on Windows | yes | unsupported: not implemented on Windows native | unsupported: no sockets |
| Named pipe (`NamedPipe`) | unsupported: one synchronous handle cannot read and write at once on Windows | yes, on Windows only | unsupported: Windows concept | unsupported: not implemented | unsupported: no pipes |
| Node IPC (`NodeIpc`) | unsupported: Node concept | yes, when started with an IPC channel | unsupported: Node concept | unsupported: Node concept | unsupported |
| Child process (`ProcessTransport`) | yes | yes | yes, POSIX only | unsupported: not implemented | unsupported: no processes |
| In memory (`InMemoryTransport.pair`) | yes | yes | yes | yes | yes |

"Implemented, not executed in CI" means the code exists but no test runs it: stdio binds the test
process's own stdin and stdout, which the test runner owns. Node IPC is tested against a stand-in
channel (an event emitter with `send` and `disconnect`), because the test process is not a forked child.
The named pipe is executed only by the Windows job, on a Windows host.

An unsupported cell throws `UnsupportedTransportException`. The Node.js modules are loaded lazily, so a
browser bundle that uses only the core APIs and the in-memory transport should not need `net`, `fs`,
`child_process`, `os`, `buffer` or `process` (not yet verified with a bundler). The lookup uses `require`,
so it assumes the default Kotlin/JS UMD module kind.

## Ownership and teardown

- A transport is a byte pipe: it never frames or parses. `incoming` has a single collector, completes
  normally on EOF or `close()`, and fails with `TransportException` on I/O errors.
- `close()` is idempotent, never blocks forever on a live peer, and signals EOF to the peer.
- A `Connection` never closes the transport it is wired to; whoever created the transport closes it.
- A `TransportListener` owns its server socket and closes it on `close()`; the owner of each accepted
  transport closes that transport. A `ProcessTransport` closes the child's stdin, waits up to its exit
  timeout, then terminates the child.

## Quick start

`Connection` is transport independent: pump bytes in with `receive` and out with `outgoing`.

```kotlin
val (clientEnd, serverEnd) = InMemoryTransport.pair()
val server = Connection()
server.onRequest("echo") { params -> params }

val scope = CoroutineScope(Dispatchers.Default)
scope.launch { serverEnd.incoming.collect { server.receive(it) } }
scope.launch { server.outgoing.collect { serverEnd.send(it) } }

val client = Connection()
scope.launch { clientEnd.incoming.collect { client.receive(it) } }
scope.launch { client.outgoing.collect { clientEnd.send(it) } }

val result = client.request<JsonElement>("echo", JsonPrimitive("hello"))
```

Over a socket, replace the in-memory pair with `TcpSocket.connect(host, port)` or
`TcpListener.bind(host, port).accept()`, and close the transports when done.

## Bounded mode and resource limits

The default connection runs handlers inline and has no ingress or egress limits. For servers that read
untrusted input, opt into bounded dispatch and limits:

```kotlin
val connection = Connection(
    policy = BoundedDispatchPolicy(maxConcurrentRequests = 8, maxQueuedMessages = 256),
    inboundLimits = InboundResourceLimits(
        maxFrameBodyBytes = 4 * 1024 * 1024,
        maxJsonNesting = 64,
        maxApplicationBodyBytes = 16 * 1024 * 1024,
    ),
    outboundLimits = outboundLimits, // OutboundResourceLimits(...)
)
```

- Requests run concurrently up to `maxConcurrentRequests`; notifications are barriers that run after
  earlier requests and before later messages.
- A request that arrives with the queue full receives a server-busy error. A notification overflow or
  a duplicate active request id fails the connection (`Connection.state` becomes `Failed`).
- `InboundResourceLimits` bounds the frame body, JSON nesting and queued application bytes.
  `OutboundResourceLimits` bounds frame size, retained bytes, outstanding frames and waiting producers.
- In bounded mode `outgoing` needs one dedicated, continuous collector.
- **Windows native limitation:** kotlinx.serialization encodes and decodes JSON trees recursively, and
  a Windows executable reserves only 1 MB of stack by default (8 MB on Linux and macOS). Documents
  nested near the 256-level maximum can overflow it. This build links its own mingwX64 executables
  with an 8 MB reservation (`-Wl,--stack,8388608`); executables that embed jsonrpc4k and accept
  deeply nested JSON should do the same.

## Cancellation

- Inbound `$/cancelRequest` cancels the handler of the request it names, which is then answered with a
  Request Cancelled error (-32800).
- **Default-mode limitation:** the default mode runs each handler to completion inside `receive`, before
  the next message is read, so an inbound cancel cannot interrupt a handler that is already running. It
  only takes effect for handlers that run concurrently, that is, in bounded mode. Use bounded mode when
  handlers must be cancellable.
- A caller of `request` that stops waiting (timeout or cancellation) sends `$/cancelRequest` for its own
  request. Requests inside a bounded-mode batch cannot be cancelled.

## Build

JDK 17 or newer is required (the JVM target uses toolchain 17).

```sh
./gradlew check                 # all targets runnable on this host, plus detekt
./gradlew jvmTest jsNodeTest    # JVM and Node.js tests
./gradlew mingwX64Test          # Windows native tests (Windows host)
./gradlew spotlessApply         # format
```

CI runs Linux, macOS and Windows jobs. There is no Linux arm64 job: Kotlin/Native 2.3.0 has no
linux-aarch64 host toolchain.
