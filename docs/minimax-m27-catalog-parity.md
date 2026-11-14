# MiniMax M2.7 catalog parity

The pinned Pi v1.5.0 catalog (`f9a26365568127461c47a07a37dcb42c140a3ac5`)
includes MiniMax-M2.7 and MiniMax-M2.7-highspeed for both the international
and China MiniMax providers. The native offline catalog includes these text-only,
reasoning-capable Messages models, their 204,800-token context window,
131,072-token output limit, and region-specific pricing. Native explicitly
selects budgeted thinking, which is Pi's default when adaptive thinking is not
specified. The existing regional endpoint and credential bindings are unchanged.

An offline CLI fixture checks catalog metadata and sends budgeted Messages
requests to a local server for each provider/model combination. No live MiniMax
service was contacted.
