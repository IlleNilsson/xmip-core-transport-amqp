# xmip-core-transport-amqp

AMQP 0-9-1 transport: one publish or delivery is one Stream, the exchange and routing key beside it; a Location consumes a queue or publishes through a broker, or accepts clients directly. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
