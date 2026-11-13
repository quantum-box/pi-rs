# MCP resource pages

For each connected MCP server with resources, the native adapter exposes
`mcp__<server>__list_resources_page` and
`mcp__<server>__list_resource_templates_page`. Call either with `{}` for the
first page, then pass its `nextCursor` as `cursor` to fetch the next page. The
response includes the server name and tags each returned entry with it. A page
may have no visible entries and still have a `nextCursor`.

These page tools inherit the exposure of their corresponding `list_resources`
or `list_resource_templates` tool, including hidden and deferred settings. They
apply the same Pi-compatible MCP App and metadata filtering as the existing
all-pages tools. Existing names continue to aggregate all pages for one server.

Unlike Pi's shared listing tools, native page tools are named per server and do
not accept a `server` argument or combine results from multiple servers.
