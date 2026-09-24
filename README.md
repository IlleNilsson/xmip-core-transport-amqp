# xmip-core-transport-amqp

AMQP 0-9-1 transport: one publish or delivery is one Stream, the exchange and routing key beside it; a Location consumes a queue or publishes through a broker, or accepts clients directly. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

This is the one AMQP 0-9-1 in the estate: `wire` (the frame, and AMQP's own
value encodings — short string, long string, field table — as the `Amqp` and
`AmqpWrite` traits over
[xmip-core-library-codec](https://github.com/IlleNilsson/xmip-core-library-codec)'s
byte cursor and writer), `method`, `content` (the header and body frames), `client` and
`session` (the far end a test or the playground stands up in place of a
broker). [xmip-core-transport-rabbitmq](https://github.com/IlleNilsson/xmip-core-transport-rabbitmq)
speaks through them rather than carrying its own.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
