---
"@amqp-contract/contract": major
---

`defineContract` now throws when a consumed queue has no dead-letter exchange,
because such a queue silently discards every message its handler rejects. Add
`deadLetter: { exchange: … }`, or `onPoison: "drop"` if the loss is deliberate.
Declared-but-unconsumed queues (including dead-letter queues themselves) are not
checked.
