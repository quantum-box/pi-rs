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

Pages larger than 512 KiB are saved to a private temporary file for the lifetime
of the connection. The response includes `outputId`, `outputBytes`, `readTool`,
and, when reasonably sized, `nextCursor`. Call the same page tool with
`{"outputId":"...","offset":0}` and continue with `nextOffset` to read the
complete JSON in UTF-8 chunks, including any exceptionally large cursor. At most
eight saved pages are retained per connection; older handles expire. This
retrieval does not require a local filesystem read tool. Like Pi, saved pages
contain the filtered listing; unlike Pi, they are read through the page tool
instead of a filesystem path.

Page serialization and file writes run on a blocking worker. Cancellation or
dropping the call stops the worker cooperatively at write boundaries and removes
unpublished temporary files. A single JSON serializer scan or filesystem write
cannot be interrupted until that operation returns.
