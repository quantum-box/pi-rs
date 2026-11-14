# MiniMax M2.7 catalog compatibility

Pinned Pi v1.5.0 (`f9a26365568127461c47a07a37dcb42c140a3ac5`) lists
MiniMax-M2.7 and MiniMax-M2.7-highspeed for both direct MiniMax regions. Pi's
Anthropic Messages adapter selects budgeted `thinking.type: enabled` for these
entries when thinking is enabled, because they do not opt into adaptive thinking.
The simple API sends `thinking.type: disabled` when reasoning is off; a lower-level
caller that leaves thinking unspecified omits the field. See the pinned
[adapter](https://github.com/earendil-works/pi/blob/f9a26365568127461c47a07a37dcb42c140a3ac5/packages/ai/src/api/anthropic-messages.ts)
and [MiniMax catalog rules](https://github.com/earendil-works/pi/blob/f9a26365568127461c47a07a37dcb42c140a3ac5/packages/ai/catalog/providers/minimax.ts).

The current [China Messages API reference](https://platform.minimax.cn/docs/api-reference/text-chat-anthropic)
lists M3 and M3.1 Flash, describes adaptive thinking for those models, and does
not document M2.7 or budgeted thinking. The [China model FAQ](https://platform.minimax.cn/docs/faq/models)
and [international model FAQ](https://platform.minimax.io/docs/faq/models) describe
M2.7 retirement and subscription routing. This does not establish a currently
supported direct M2.7 Messages route or budgeted thinking contract.

The native direct catalogs therefore continue to withhold those entries, as
recorded in `native-parity.md`; manually configured models retain existing
behavior. No live provider was contacted, and a loopback response cannot prove
provider support. A regression keeps the withheld entries out of both direct
catalogs while preserving the listed M3 models.
