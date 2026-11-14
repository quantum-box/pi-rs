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
    let storedChars = Object.entries(values).reduce((size, [key, value]) => size + key.length + value.length, 0);
    let changed = false;
    const checkKey = (name, key) => {
        if (typeof key !== "string") throw new TypeError(name + "() key must be a string");
    };
    globalThis.load = key => {
        checkKey("load", key);
        return Object.hasOwn(values, key) ? JSON.parse(values[key]) : undefined;
    };
    globalThis.store = (key, value) => {
        checkKey("store", key);
        if (value === undefined) {
            const previous = Object.hasOwn(values, key) ? key.length + values[key].length : 0;
            delete values[key];
            storedChars -= previous;
            changed = true;
            return;
        }
        let encoded;
        try { encoded = JSON.stringify(value); }
        catch (_) { throw new TypeError("stored value is not JSON serializable"); }
        if (encoded === undefined) throw new TypeError("stored value is not JSON serializable");
        if (encoded.length > 262144) throw new RangeError("stored value exceeds size limit");
        // JSON.stringify can invoke toJSON, which may mutate this same key.
        const previous = Object.hasOwn(values, key) ? key.length + values[key].length : 0;
        const size = storedChars - previous + key.length + encoded.length;
        if (size > 1048576) throw new RangeError("store exceeds size limit");
        values[key] = encoded;
        storedChars = size;
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
    const record = value => typeof value === "object" && value !== null && !Array.isArray(value);
    const imageBlock = value => record(value) && value.type === "image" && typeof value.data === "string" && typeof value.mimeType === "string";
    const validateContext = (action, context) => {
        const name = action === "classify" ? "models.classify()" : "models.generateImages()";
        if (!record(context)) throw new Error(`${name} expects a context object`);
        if (action === "classify") {
            if (!record(context.state)) throw new Error(`${name} context.state must be an object`);
            if (context.images !== undefined && (!Array.isArray(context.images) || !context.images.every(imageBlock)))
                throw new Error(`${name} context.images must be an array of image blocks`);
            if (!record(context.questions) || !Object.keys(context.questions).length)
                throw new Error(`${name} context.questions must map question IDs to questions`);
            for (const [id, question] of Object.entries(context.questions)) {
                if (!record(question) || typeof question.instructions !== "string")
                    throw new Error(`${name} context.questions.${id} must have string instructions`);
                const criteria = question.criteria;
                const strings = values => values.length > 0 && values.every(value => typeof value === "string");
                const valid = question.type === "choice" ? record(criteria) && strings(Object.values(criteria))
                    : question.type === "score" ? Array.isArray(criteria) && strings(criteria)
                    : question.type === "bool" ? record(criteria) && typeof criteria.true === "string" && typeof criteria.false === "string"
                    : false;
                if (!valid) throw new Error(`${name} context.questions.${id} has invalid type or criteria`);
            }
        } else if (!Array.isArray(context.input) || !context.input.length || !context.input.every(value =>
            record(value) && ((value.type === "text" && typeof value.text === "string") || imageBlock(value)))) {
            throw new Error(`${name} context.input must be a non-empty array of text or image blocks`);
        }
    };
    const modelOperation = async (action, model, context) => {
        if (!record(model) || typeof model.provider !== "string" || typeof model.id !== "string")
            throw new Error(`models.${action}() expects a model with provider and id strings`);
        validateContext(action, context);
        const { provider, id } = model;
        const reply = JSON.parse(await callModel(JSON.stringify({ action, provider, id, context })));
        if (!reply.ok) throw new Error(reply.error);
        return reply.value;
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
