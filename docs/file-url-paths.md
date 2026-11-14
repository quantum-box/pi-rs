# File URL paths

The local file tools resolve `file://` URLs using Pi's Node `fileURLToPath`
validation. Encoded `/` is rejected on all platforms; encoded `\` is also
rejected on Windows. Malformed percent escapes and decoded paths that are not
valid UTF-8 are rejected before filesystem access. Normal encoded spaces and
Unicode remain supported, and query and fragment text does not become part of
the path.

These checks apply to local and injected backend path normalization. Scoped
workspaces still perform their existing containment checks after normalization;
remote hosts remain responsible for their backend access policy.
