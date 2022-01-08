# Getting started

This project encompasses:

1. A Java client for the Riemann protocol
2. The Riemann protocol buffer definition, and
3. Its corresponding (auto-generated) Java classes

[![Clojars
Project](https://img.shields.io/clojars/v/io.riemann/riemann-java-client.svg)](https://clojars.org/io.riemann/riemann-java-client)

[![CircleCI](https://circleci.com/gh/riemann/riemann-java-client.svg?style=svg)](https://circleci.com/gh/riemann/riemann-java-client)

## Artifacts

Artifacts are available through
[clojars](https://clojars.org/io.riemann/riemann-java-client) which you can add
to your maven repository like so:

```xml
<repository>
  <id>clojars.org</id>
  <url>http://clojars.org/repo</url>
</repository>
```

**Note:**
The namespace for the client was previously `com.aphyr` but has been
renamed to `io.riemann` since the 0.4.2 release. You need to update your
dependencies.

## Example

``` java
RiemannClient c = RiemannClient.tcp("my.riemann.server", 5555);
c.connect();
c.event().
  service("fridge").
  state("running").
  metric(5.3).
  tags("appliance", "cold").
  send().
  deref(5000, java.util.concurrent.TimeUnit.MILLISECONDS);

c.query("tagged \"cold\" and metric > 0").deref(); // => List<Event>;
c.close();
```

Clients will automatically attempt to reconnect every 5 seconds. Writes
will fail instantaneously when no connection is available.

`.send()` proceeds asynchronously and returns as soon as Netty flushes
the write possible. `.send()` returns a
`io.riemann.riemann.client.IPromise` containing the response from the
write (which also supports Clojure's Deref protocol). If you do not
deref this promise, the client makes *no* guarantees about event
delivery: it will, for example, discard writes when there are too many
messages outstanding on the wire, when Riemann cannot keep up with load,
and so on. You *should* deref sends at some point, if for no other
reason than to handle backpressure.

Calling `.deref()` will throw a ServerError if the server responds with
an error, or other Runtime/IOExceptions for error conditions, like a
channel being disconnected, etc.

```java
try {
  if (!c.event().
      service("fridge").
      state("running").
      send().
      deref(1, java.util.concurrent.TimeUnit.SECONDS)) {
    throw new IOException("Timed out.")
  }
} catch (Exception e) {
  retry();
}
```

This code blocks for 1 second before retrying, returns a Message if the
write succeeded, null if the promise is still outstanding, and throws if
a failure is known to have occurred. This means you can send multiple
copies of an event if latencies exceed 1000 ms. There is no reliable way
to distinguish between failure and delay in an asynchronous network, so
think ahead. `.deref()` blocks indefinitely, but will return as soon as
the Netty connection fails, so it may be the safest option when
arbitrary delays are acceptable.

Each client allows thousands of outstanding concurrent requests at any
time, so a small number of threads can efficiently pipeline many
operations over the same client. I suggest performing writes on a
special monitoring thread or threads, and pushing the response futures
onto a threadpoolexecutor for `deref`ing.

For higher performance (by orders of magnitude) you can also send
multiple events batched in a single message. Use
`RiemannClient.sendEvents(...)` to send multiple events at once.

To automatically batch events, wrap any IRiemannClient in a
RiemannBatchClient, which automatically bundles events together into
messages for you.

# Release and deployment

1. Increment the version.

2. Compile the client.

```sh
mvn compile -rf :riemann-java-client
```

3. Deploy the client.

```sh
mvn deploy -tf :riemann-java-client-parent
mvn deploy -rf :riemann-java-client
```

# Hacking

You'll need [protobuf 3.16.1](https://github.com/google/protobuf) - you
can just install the `protoc` binary. After that, `mvn package` should
build a JAR, and `mvn install` will drop it in your local repository.
