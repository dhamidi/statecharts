package ecmascript

const bridgeSource = `
(() => {
  "use strict";

  const hostEvent = globalThis.__statecharts_event;
  const hostSessionID = globalThis.__statecharts_session_id;
  const hostName = globalThis.__statecharts_name;
  const hostIOProcessors = globalThis.__statecharts_ioprocessors;
  const hostPlatform = globalThis.__statecharts_platform;
  const hostIn = globalThis.__statecharts_in;
  const taggedMarker = Symbol("statecharts.tagged");
  let data = Object.create(null);
  let globalBaseline = null;

  function validUnicode(value) {
    for (let i = 0; i < value.length; i++) {
      const code = value.charCodeAt(i);
      if (code >= 0xd800 && code <= 0xdbff) {
        if (++i >= value.length) return false;
        const next = value.charCodeAt(i);
        if (next < 0xdc00 || next > 0xdfff) return false;
      } else if (code >= 0xdc00 && code <= 0xdfff) {
        return false;
      }
    }
    return true;
  }

  function decimalParts(text) {
    if (typeof text !== "string") throw new TypeError("invalid Value number");
    const match = /^(-?)([0-9]+)(?:\.([0-9]+))?(?:[eE]([+-]?[0-9]+))?$/.exec(text);
    if (match === null) throw new TypeError("invalid Value number");
    let digits = match[2] + (match[3] || "");
    let exponent = BigInt(match[4] || "0") - BigInt((match[3] || "").length);
    digits = digits.replace(/^0+/, "");
    if (digits === "") return {negative: false, digits: "0", exponent: 0n};
    while (digits.endsWith("0")) {
      digits = digits.slice(0, -1);
      exponent++;
    }
    return {negative: match[1] === "-", digits, exponent};
  }

  function normalizedDecimal(text) {
    const parts = decimalParts(text);
    if (parts.digits === "0") return "0";
    return (parts.negative ? "-" : "") + parts.digits + "e" + String(parts.exponent);
  }

  function importNumber(text) {
    const number = Number(text);
    if (!Number.isFinite(number)) throw new RangeError("Value number is outside the ECMAScript finite range");
    if (normalizedDecimal(text) === normalizedDecimal(String(number))) return number;
    const parts = decimalParts(text);
    if (parts.exponent < 0n) {
      throw new RangeError("Value number cannot be represented exactly by ECMAScript");
    }
    const magnitude = BigInt(parts.digits) * (10n ** parts.exponent);
    return parts.negative ? -magnitude : magnitude;
  }

  function importWire(wire) {
    if (!wire || wire.version !== 1 || typeof wire.kind !== "string") {
      throw new TypeError("invalid statecharts Value wire");
    }
    switch (wire.kind) {
      case "null": return null;
      case "bool": return wire.bool;
      case "string":
        if (typeof wire.string !== "string" || !validUnicode(wire.string)) throw new TypeError("invalid Value string");
        return wire.string;
      case "number": return importNumber(wire.number);
      case "list": return wire.list.map(importWire);
      case "map": {
        const result = Object.create(null);
        for (const key of Object.keys(wire.map)) {
          if (!validUnicode(key)) throw new TypeError("invalid Value map key");
          result[key] = importWire(wire.map[key]);
        }
        return result;
      }
      case "tagged": {
        if (typeof wire.tag !== "string" || !validUnicode(wire.tag)) throw new TypeError("invalid Value tag");
        const result = Object.create(null);
        Object.defineProperty(result, taggedMarker, {value: true});
        Object.defineProperties(result, {
          tag: {value: wire.tag, enumerable: true},
          value: {value: importWire(wire.payload), enumerable: true},
        });
        return result;
      }
      default: throw new TypeError("unknown statecharts Value kind " + wire.kind);
    }
  }

  function exportWire(value, seen) {
    if (value === null) return {version: 1, kind: "null"};
    switch (typeof value) {
      case "boolean": return {version: 1, kind: "bool", bool: value};
      case "string":
        if (!validUnicode(value)) throw new TypeError("ECMAScript string contains an unpaired surrogate");
        return {version: 1, kind: "string", string: value};
      case "number":
        if (!Number.isFinite(value)) throw new TypeError("non-finite ECMAScript number cannot become a statecharts Value");
        return {version: 1, kind: "number", number: String(value)};
      case "bigint":
        return {version: 1, kind: "number", number: String(value)};
      case "undefined":
      case "function":
      case "symbol":
        throw new TypeError(typeof value + " cannot become a statecharts Value");
      case "object":
        break;
      default:
        throw new TypeError(typeof value + " cannot become a statecharts Value");
    }
    if (value instanceof Promise) throw new TypeError("Promise results are not statechart values");
    if (seen.has(value)) throw new TypeError("cyclic ECMAScript value cannot become a statecharts Value");
    seen.add(value);
    try {
      if (value[taggedMarker] === true) {
        return {version: 1, kind: "tagged", tag: value.tag, payload: exportWire(value.value, seen)};
      }
      if (Array.isArray(value)) {
		const list = [];
		for (let i = 0; i < value.length; i++) {
		  if (!Object.prototype.hasOwnProperty.call(value, i)) throw new TypeError("sparse ECMAScript array cannot become a statecharts Value");
		  const descriptor = Object.getOwnPropertyDescriptor(value, String(i));
		  if (descriptor.get !== undefined || descriptor.set !== undefined) throw new TypeError("ECMAScript array accessors cannot become a statecharts Value");
		  list.push(exportWire(descriptor.value, seen));
		}
		return {version: 1, kind: "list", list};
      }
      const prototype = Object.getPrototypeOf(value);
      if (prototype !== Object.prototype && prototype !== null) {
        throw new TypeError("unsupported ECMAScript object cannot become a statecharts Value");
      }
      const result = Object.create(null);
      for (const key of Object.keys(value)) {
        if (!validUnicode(key)) throw new TypeError("ECMAScript object key contains an unpaired surrogate");
		const descriptor = Object.getOwnPropertyDescriptor(value, key);
		if (descriptor.get !== undefined || descriptor.set !== undefined) throw new TypeError("ECMAScript object accessors cannot become a statecharts Value");
		result[key] = exportWire(descriptor.value, seen);
      }
      return {version: 1, kind: "map", map: result};
    } finally {
      seen.delete(value);
    }
  }

  function deepFreeze(value, seen = new WeakSet()) {
    if (value === null || typeof value !== "object" || seen.has(value)) return value;
    seen.add(value);
    for (const key of Object.keys(value)) deepFreeze(value[key], seen);
    return Object.freeze(value);
  }

  function readonly(name, get) {
    Object.defineProperty(globalThis, name, {
      configurable: false,
      enumerable: false,
      get,
      set() { throw new TypeError(name + " is a protected system binding"); },
    });
  }

  const __sc_import = text => importWire(JSON.parse(text));
  const __sc_export = value => JSON.stringify(exportWire(value, new Set()));
  const __sc_declare = (name, direct) => {
    data[name] = null;
    if (!direct) return;
    if (Object.prototype.hasOwnProperty.call(globalThis, name)) {
      throw new TypeError("data ID conflicts with ECMAScript global: " + name);
    }
    Object.defineProperty(globalThis, name, {
      configurable: false,
      enumerable: true,
      get() { return data[name]; },
      set(value) { data[name] = value; },
    });
  };
  const __sc_read_data = name => data[name];
  const __sc_assign_data = (name, value) => { data[name] = value; };
  const __sc_restore = text => {
    const values = JSON.parse(text);
    const replacement = Object.create(null);
    for (const item of values) replacement[item.id] = importWire(item.value);
    data = replacement;
  };
  const __sc_capture_globals = () => {
    globalBaseline = Reflect.ownKeys(globalThis).map(key => {
      const descriptor = Object.getOwnPropertyDescriptor(globalThis, key);
      return {key, descriptor};
    });
  };
  const __sc_globals_unchanged = () => {
    if (globalBaseline === null) return false;
    const keys = Reflect.ownKeys(globalThis);
    if (keys.length !== globalBaseline.length) return false;
    for (let i = 0; i < keys.length; i++) {
      const baseline = globalBaseline[i];
      if (keys[i] !== baseline.key) return false;
      const current = Object.getOwnPropertyDescriptor(globalThis, keys[i]);
      const previous = baseline.descriptor;
      if (current.configurable !== previous.configurable ||
          current.enumerable !== previous.enumerable ||
          current.writable !== previous.writable ||
          !Object.is(current.value, previous.value) ||
          current.get !== previous.get || current.set !== previous.set) return false;
    }
    return true;
  };

  Object.defineProperties(globalThis, {
    __sc_import: {value: __sc_import},
    __sc_export: {value: __sc_export},
    __sc_declare: {value: __sc_declare},
    __sc_read_data: {value: __sc_read_data},
    __sc_assign_data: {value: __sc_assign_data},
    __sc_restore: {value: __sc_restore},
    __sc_capture_globals: {value: __sc_capture_globals},
    __sc_globals_unchanged: {value: __sc_globals_unchanged},
  });

  readonly("$data", () => data);
  readonly("_event", () => {
    const wire = hostEvent();
    return wire === "" ? undefined : deepFreeze(__sc_import(wire));
  });
  readonly("_sessionid", () => hostSessionID());
  readonly("_name", () => hostName());
  readonly("_ioprocessors", () => deepFreeze(__sc_import(hostIOProcessors())));
  readonly("_x", () => deepFreeze(__sc_import(hostPlatform())));
  readonly("In", () => function(state) { return hostIn(state) !== 0; });
})();
`
