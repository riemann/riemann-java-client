1.5.0
=====

* Upgrade to Netty 4

NOTES:


- See [Netty 4](http://netty.io/wiki/new-and-noteworthy-in-4.0.html) upgrade notes


- As [mentioned](http://netty.io/wiki/new-and-noteworthy-in-4.0.html#write-does-not-flush-automatically)
  Netty 4 won't flush sends automatically. By default this client will flush after each message,
  this can be controlled via the `autoFlush` field on the Transports.

- `Resolver` class was removed. Netty will resolve on each new channel when the
  supplied address is created via `InetSocketAddress.createUnresolved`.

- use of `HashedWheelTimer` has been replaced by the Executor provided by the
  `ChannelHandlerContext`

- Previous versions used to explicitly disable SSL TLS renegotiation. This no longer appears to be part of Netty.
  [Stack Overflow](https://stackoverflow.com/questions/31418644/is-it-possible-to-disable-tls-renegotiation-in-netty-4)
  suggests setting the JDK8+ System Property `jdk.tls.rejectClientInitiatedRenegotiation`