# 0027: One HTTP Transport handle

Supersedes the HTTP `Io` / `Transport` split in decisions 0024 and 0025.

`Transport` is one concrete handle accepted by both orientations of
`Format<Transport, R, W>`. `Transport::accept` wraps an incoming exchange;
`Transport::new` wraps an async sending function. There is no public `Io`
and no outbound transport trait. Existing HTTP, WebSocket, and stdio
backends convert into the handle with `From`.

`connect(transport)` shares the sender and creates a handle carrying each
attempt’s final extensions. Constructing and binding the handle performs
no I/O. The format starts sending when its outgoing sink is driven.
Clones share outbound resources; an accepted exchange can bind exactly
once across its clones. Heads remain inside the HTTP boundary.

Core still selects each edge’s type orientation and remains generic over
its transport. No transport type or HTTP state is introduced into core.
