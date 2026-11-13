(() => {
    const stringify = value => typeof value === "string" ? value : JSON.stringify(value) ?? String(value);
    const emit = (kind, value, mimeType = "") => {
        const error = __emit(kind, value, mimeType);
        if (error) throw new Error(error);
    };
    globalThis.text = value => emit("text", stringify(value));
    globalThis.image = value => {
        if (typeof value === "object" && value !== null && "image_url" in value) value = value.image_url;
        if (typeof value === "string") {
            const match = /^data:(image\/(?:png|jpeg|gif|webp));base64,([A-Za-z0-9+/=\s]+)$/.exec(value);
            if (!match) throw new Error("image requires a PNG, JPEG, GIF or WebP data URL");
            value = { type: "image", mimeType: match[1], data: match[2].replace(/\s/g, "") };
        }
        if (!value || value.type !== "image" || typeof value.data !== "string") throw new Error("invalid image block");
        emit("image", value.data, typeof value.mimeType === "string" ? value.mimeType : "");
    };
    globalThis.console = Object.freeze(Object.fromEntries(["log", "info", "warn", "error", "debug"].map(name => [name, (...args) => emit("console", args.map(stringify).join(" "))])));
    const values = Object.create(null);
    for (const [key, value] of Object.entries(__initial_store)) values[key] = JSON.stringify(value);
    let changed = false;
    globalThis.load = key => Object.hasOwn(values, String(key)) ? JSON.parse(values[String(key)]) : undefined;
    globalThis.store = (key, value) => {
        key = String(key);
        if (key.length > 1024) throw new Error("store key exceeds size limit");
        const encoded = value === undefined ? undefined : JSON.stringify(value);
        if (value !== undefined && encoded === undefined) throw new Error("stored value is not JSON serializable");
        if (encoded !== undefined && encoded.length > 262144) throw new Error("stored value exceeds size limit");
        const next = { ...values };
        if (encoded === undefined) delete next[key];
        else next[key] = encoded;
        const size = Object.entries(next).reduce((sum, [name, entry]) => sum + name.length + entry.length, 0);
        if (size > 1048576) throw new Error("store exceeds size limit");
        if (encoded === undefined) delete values[key];
        else values[key] = encoded;
        changed = true;
    };
    const tools = Object.create(null);
    const aliases = new Map();
    for (const tool of __catalog) {
        const call = async (args = {}) => {
            const result = JSON.parse(await __call_tool(tool.name, JSON.stringify(args)));
            if (!result.ok) throw new Error(result.error);
            return result.value;
        };
        tools[tool.name] = call;
        const alias = tool.name.replace(/[^A-Za-z0-9_$]/g, "_");
        if (aliases.has(alias) && aliases.get(alias) !== tool.name) aliases.set(alias, null);
        else aliases.set(alias, tool.name);
    }
    for (const [alias, original] of aliases) {
        if (!(alias in tools)) tools[alias] = original === null ? () => { throw new Error("ambiguous tool alias"); } : tools[original];
    }
    globalThis.tools = Object.freeze(tools);
    globalThis.ALL_TOOLS = Object.freeze(__catalog.map(({ name, description }) => Object.freeze({ name, description })));
    globalThis.searchTools = async (query, options = {}) => JSON.parse(__search_tools(String(query), JSON.stringify(options)));
    globalThis.describeTool = async name => JSON.parse(__describe_tool(String(name))) ?? undefined;
    globalThis.describeNamespace = async name => JSON.parse(__describe_namespace(String(name))) ?? undefined;
    const callModel = __call_model_operation;
    const modelType = type => {
        if (!["chat", "classifier", "image"].includes(type)) throw new Error("Unknown model type");
        return type;
    };
    const listModels = async (type, provider, available, id) => {
        const reply = JSON.parse(await callModel(JSON.stringify({ action: "list", type: modelType(type), provider, available, id })));
        if (!reply.ok) throw new Error(reply.error);
        return reply.value;
    };
    const modelOperation = async (action, model, context) => {
        const { provider = "", id = "" } = model ?? {};
        const reply = JSON.parse(await callModel(JSON.stringify({ action, provider, id, context })));
        return reply.ok ? reply.value : { provider, model: id, stopReason: "error", errorMessage: reply.error, ...(action === "classify" ? { answers: {} } : { output: [] }) };
    };
    globalThis.models = Object.freeze({
        getModelsOfType: (type, provider) => listModels(type, provider, false),
        getAvailableOfType: (type, provider) => listModels(type, provider, true),
        getModelOfType: async (type, provider, id) => (await listModels(type, provider, false, id))[0],
        classify: (model, context) => modelOperation("classify", model, context),
        generateImages: (model, context) => modelOperation("generateImages", model, context),
    });
    const exitToken = Symbol("exit");
    globalThis.exit = () => { throw exitToken; };
    globalThis.__run = async action => {
        let ok = true;
        let error;
        try {
            const result = await action();
            if (result !== undefined) text(result);
        } catch (caught) {
            if (caught !== exitToken) {
                ok = false;
                error = String(caught) + (caught?.stack ? "\n" + caught.stack : "");
            }
        }
        return JSON.stringify({ ok, error, changed, store: Object.fromEntries(Object.entries(values).map(([key, value]) => [key, JSON.parse(value)])) });
    };
})();
