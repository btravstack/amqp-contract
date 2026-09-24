---
"@amqp-contract/core": major
"@amqp-contract/client": major
"@amqp-contract/worker": major
---

One message codec, in core. JSON encoding, compression, decompression and the
inbound size guard now live in a single module that the client (publish and
RPC replies) and the worker (consume) share, instead of three copies that could
drift apart.

Breaking: the inbound size cap drops from 64 MiB to **16 MiB**
(`DEFAULT_MAX_MESSAGE_BYTES`, exported from core — RabbitMQ 4's own default
`max_message_size`), and it now applies to **uncompressed** bodies too, not
only to what a compressed body inflates to. An over-cap message is a defect and
follows the poison-message DLQ path. Raise it with the worker's
`maxMessageBytes` option if you publish larger messages (`maxDecompressedBytes`
still works as a deprecated alias).

RPC replies are decoded through the same codec, so a reply carrying a
`contentEncoding` is now decompressed rather than failing to parse.
