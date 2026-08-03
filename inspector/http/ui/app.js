const TIMELINE_LIMIT = 200;
const RECENT_PAGE_LIMIT = 1000;
const SEEN_SEQUENCE_LIMIT = 1000;
let eventSuggestionSequence = 0;

const el = (tag, attrs = {}, ...children) => {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else if (value !== undefined) node.setAttribute(key, value);
  }
  for (const child of children.flat()) {
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
};

const wireNames = {
  id: "ID",
  chartID: "ChartID",
  sessionID: "SessionID",
  sendID: "SendID",
  invokeID: "InvokeID",
  deliveryID: "DeliveryID",
};
const get = (object, key) => object?.[key] ?? object?.[wireNames[key] || key[0].toUpperCase() + key.slice(1)];
const field = (label, value) => [el("dt", {}, label), el("dd", {}, value ?? "—")];
const compact = value => {
  const text = String(value ?? "—");
  return text.length > 28 ? `${text.slice(0, 16)}…${text.slice(-8)}` : text;
};

const relativeSeconds = timestamp => {
  const elapsed = Math.max(0, (Date.now() - Date.parse(timestamp)) / 1000);
  if (!Number.isFinite(elapsed)) return "time unknown";
  return `${elapsed < 1 ? elapsed.toFixed(2) : elapsed < 10 ? elapsed.toFixed(1) : Math.round(elapsed)}s ago`;
};

const eventTypeName = type => ["external", "internal", "platform"][Number(type)] || String(type ?? "unknown");

const macrostepSummary = macrostep => {
  const trigger = get(macrostep, "trigger");
  const before = (get(macrostep, "before") || []).map(String);
  const after = (get(macrostep, "after") || []).map(String);
  const microsteps = get(macrostep, "microsteps") || [];
  const parts = [trigger ? `${get(trigger, "name") || "(unnamed event)"} [${eventTypeName(get(trigger, "type"))}]` : "initialization"];
  const sameConfiguration = before.length === after.length && before.every((state, index) => state === after[index]);
  parts.push(sameConfiguration ? `state unchanged: ${after.join(", ") || "empty"}` : `${before.join(", ") || "∅"} → ${after.join(", ") || "∅"}`);
  const triggerName = String(get(trigger, "name") || "");
  const followups = [...new Set(microsteps.map(step => String(get(get(step, "trigger"), "name") || "")).filter(name => name && name !== triggerName))];
  if (followups.length) parts.push(`then ${followups.join(", ")}`);
  if (get(macrostep, "terminal")) parts.push(get(macrostep, "terminalError") ? `terminal: ${get(macrostep, "terminalError")}` : "terminal");
  return parts.join(" · ");
};

const macrostepTransitionRefs = macrostep => (get(macrostep, "microsteps") || []).flatMap(step => get(step, "transitions") || [])
  .map(ref => ({ source: String(get(ref, "source")), index: get(ref, "index") }));

const observationCategory = kind => {
  if (kind === "macrostep") return "macrostep";
  if (kind === "residency.changed") return "residency";
  if (kind === "actor.discovered" || kind === "actor.terminal") return "lifecycle";
  return kind;
};

const observationSummary = (kind, actor, macrostep) => {
  if (macrostep) return macrostepSummary(macrostep);
  if (kind === "residency.changed") return `residency changed to ${get(actor, "residency") || "unknown"}`;
  if (kind === "actor.discovered") return "actor discovered";
  if (kind === "actor.terminal") return "actor reached a terminal state";
  return kind;
};

const copyableValue = (value, label) => {
  const text = String(value ?? "—");
  const feedback = el("span", { class: "copy-feedback", "aria-live": "polite" });
  const button = el("button", { type: "button", class: "copy-value", "aria-label": `Copy full ${label}`, onclick: async () => {
    try {
      await navigator.clipboard.writeText(text);
      feedback.textContent = "Copied";
    } catch (_) {
      feedback.textContent = "Copy failed — select the value to copy it.";
    }
  } }, "Copy");
  return el("span", { class: "full-value", title: text },
    el("span", { class: "identifier", tabindex: "0", "aria-label": `${label}: ${text}` }, compact(text)), button, feedback);
};

const api = async (path, options) => {
  const response = await fetch(path, options);
  const body = await response.json();
  if (!response.ok) throw Error(body.error?.message || `HTTP ${response.status}`);
  return body.data;
};

export function canonicalValue(kind = "null") {
  const value = { version: 1, kind };
  if (kind === "bool") value.bool = false;
  if (kind === "number") value.number = "0";
  if (kind === "string") value.string = "";
  if (kind === "list") value.list = [];
  if (kind === "map") value.map = {};
  if (kind === "tagged") {
    value.tag = "app";
    value.payload = canonicalValue();
  }
  return value;
}

export function actorQuery(system, filters = {}, after = "") {
  const query = new URLSearchParams({ system, limit: "50" });
  for (const [key, value] of Object.entries(filters)) if (value !== "") query.set(key, value);
  if (after) query.set("after", after);
  return `v1/actors?${query}`;
}

export function stateID(state) {
  const id = get(state, "id");
  if (typeof id === "string") return id || "(root)";
  const value = get(id, "value");
  if (value) return value;
  return get(id, "generated") ? "(generated state)" : "(root)";
}

const stateKind = value => ["atomic", "compound", "parallel", "final", "history"][Number(value)] || String(value ?? "unknown");

export function understoodEvents(definition) {
  const names = new Set();
  const visit = state => {
    if (!state) return;
    for (const transition of get(state, "transitions") || []) {
      for (const descriptor of get(transition, "events") || []) {
        const name = String(descriptor).trim();
        if (name && !name.includes("*")) names.add(name);
      }
    }
    for (const child of get(state, "children") || []) visit(child);
  };
  visit(get(definition, "root"));
  return [...names].sort((left, right) => left.localeCompare(right));
}

class CanonicalValue extends HTMLElement {
  set value(value) {
    this._value = value;
    this.render();
  }

  render() {
    this.replaceChildren(this.node(this._value, 0));
  }

  composite(kind, count, depth, content) {
    return el("details", { class: `canonical ${kind}`, ...(depth <= 2 ? { open: "" } : {}) },
      el("summary", {}, `${kind} · ${count}`), content);
  }

  node(value, depth) {
    if (!value || !get(value, "kind")) return el("span", { class: "muted" }, "—");
    const kind = get(value, "kind");
    if (kind === "null") return el("span", { class: "canonical scalar" }, "null");
    if (kind === "bool" || kind === "number" || kind === "string") {
      return el("span", { class: "canonical scalar" }, `${kind}: ${String(get(value, kind))}`);
    }
    if (kind === "list") {
      const list = get(value, "list") || [];
      return this.composite("list", `${list.length} item(s)`, depth,
        el("ol", {}, ...list.map(item => el("li", {}, this.node(item, depth + 1)))));
    }
    if (kind === "map") {
      const entries = Object.entries(get(value, "map") || {});
      return this.composite("map", `${entries.length} entr${entries.length === 1 ? "y" : "ies"}`, depth,
        el("dl", {}, ...entries.flatMap(([key, item]) => [el("dt", {}, key), el("dd", {}, this.node(item, depth + 1))])));
    }
    if (kind === "tagged") {
      const tag = get(value, "tag") || "—";
      return this.composite("tagged", tag, depth, this.node(get(value, "payload"), depth + 1));
    }
    return el("span", { class: "muted" }, `Unknown canonical kind: ${kind}`);
  }
}
customElements.define("canonical-value", CanonicalValue);

class ValueEditor extends HTMLElement {
  set value(value) {
    this._value = value || canonicalValue();
    this.render();
  }
  get value() { return this._value; }

  changed() { this.dispatchEvent(new Event("value-change", { bubbles: true })); }

  child(value, update) {
    const child = document.createElement("value-editor");
    child.setAttribute("data-nested", "");
    child.value = value;
    child.addEventListener("value-change", () => {
      // A render can detach an entire recursive editor tree. Events already
      // queued by that old tree must not modify the replacement value.
      if (child.isConnected && this.contains(child)) update(child.value);
    });
    return child;
  }

  render() {
    this.replaceChildren();
    const value = this._value;
    const nested = this.hasAttribute("data-nested");
    const kinds = ["null", "bool", "number", "string", "list", "map", "tagged"];
    const kind = el("select", {
      class: "payload-kind",
      "aria-label": "Payload type",
      onchange: event => { this.value = canonicalValue(event.target.value); this.changed(); },
    }, ...kinds.map(name => el("option", { value: name, ...(name === value.kind ? { selected: "" } : {}) }, name)));
    const row = el("div", { class: "payload-row" }, nested ? kind : el("label", { class: "payload-type-label" }, el("span", {}, "Type"), kind));

    if (value.kind === "bool") {
      row.append(el("select", { class: "payload-value", "aria-label": "Boolean value", onchange: event => { value.bool = event.target.value === "true"; this.changed(); } },
        el("option", { value: "false" }, "false"),
        el("option", { value: "true", ...(value.bool ? { selected: "" } : {}) }, "true")));
    }
    if (value.kind === "number" || value.kind === "string") {
      row.append(el("input", { class: "payload-value", value: value[value.kind] ?? "", placeholder: value.kind === "number" ? "0" : "Value", "aria-label": `${value.kind} value`, oninput: event => { value[value.kind] = event.target.value; this.changed(); } }));
    }
    if (value.kind === "tagged") {
      row.append(el("input", { class: "payload-value", value: value.tag || "", placeholder: "Application tag", "aria-label": "Payload tag", oninput: event => { value.tag = event.target.value; this.changed(); } }));
    }
    if (value.kind === "null" && !nested) row.append(el("span", { class: "null-hint" }, "No event data"));

    const box = el("div", { class: "value-editor" }, row);
    this.append(box);
    if (value.kind === "list") {
      const collection = el("div", { class: "collection list-collection" },
        el("div", { class: "collection-columns list-columns", "aria-hidden": "true" }, el("span", {}, "#"), el("span", {}, "Type and value"), el("span")));
      value.list.forEach((item, index) => collection.append(el("div", { class: "value-entry" }, el("span", { class: "item-index" }, index + 1),
        this.child(item, next => { value.list[index] = next; this.changed(); }),
        el("button", { class: "remove-entry", type: "button", title: "Remove item", "aria-label": `Remove list item ${index + 1}`, onclick: () => { value.list.splice(index, 1); this.render(); this.changed(); } }, "×"))));
      if (!value.list.length) collection.append(el("p", { class: "collection-empty" }, "No items yet."));
      collection.append(el("button", { class: "collection-add", type: "button", onclick: () => { value.list.push(canonicalValue()); this.render(); this.changed(); } }, "+ Add item"));
      box.append(collection);
    }
    if (value.kind === "map") this.renderMap(box, value.map);
    if (value.kind === "tagged") box.append(el("div", { class: "tagged-payload" },
      el("span", { class: "nested-label" }, "Tagged value"),
      this.child(value.payload, next => { value.payload = next; this.changed(); })));
  }

  renderMap(box, map) {
    const collection = el("div", { class: "collection map-collection" },
      el("div", { class: "collection-columns map-columns", "aria-hidden": "true" }, el("span", {}, "Key"), el("span", {}, "Type and value"), el("span")));
    for (const [key, item] of Object.entries(map)) {
      const errorID = `map-error-${Math.random().toString(36).slice(2)}`;
      const message = el("span", { class: "map-error", id: errorID });
      const child = this.child(item, next => {
        if (!Object.hasOwn(map, key)) return;
        Object.defineProperty(map, key, { value: next, writable: true, configurable: true, enumerable: true });
        this.changed();
      });
      const keyInput = el("input", { value: key, "aria-label": "Map key", "aria-describedby": errorID });
      keyInput.addEventListener("change", () => {
        const next = keyInput.value;
        if (next.length === 0) {
          message.textContent = "Map keys cannot be empty.";
          keyInput.setAttribute("aria-invalid", "true");
          return;
        }
        if (next !== key && Object.hasOwn(map, next)) {
          message.textContent = `Key “${next}” already exists.`;
          keyInput.setAttribute("aria-invalid", "true");
          return;
        }
        const current = map[key];
        delete map[key];
        Object.defineProperty(map, next, { value: current, writable: true, configurable: true, enumerable: true });
        message.textContent = "";
        keyInput.removeAttribute("aria-invalid");
        this.render();
        this.changed();
      });
      collection.append(el("div", { class: "map-entry" }, keyInput, child,
        el("button", { class: "remove-entry", type: "button", title: "Remove field", "aria-label": `Remove map entry “${key}”`, onclick: () => { delete map[key]; this.render(); this.changed(); } }, "×"), message));
    }
    if (!Object.keys(map).length) collection.append(el("p", { class: "collection-empty" }, "No fields yet."));
    collection.append(el("button", { class: "collection-add", type: "button", onclick: () => {
      let key = "key";
      let suffix = 2;
      while (Object.hasOwn(map, key)) key = `key${suffix++}`;
      Object.defineProperty(map, key, { value: canonicalValue(), writable: true, configurable: true, enumerable: true });
      this.render();
      this.changed();
    } }, "+ Add field"));
    box.append(collection);
  }
}
customElements.define("value-editor", ValueEditor);

class ActorDirectory extends HTMLElement {
  connectedCallback() {
    this.filters = { prefix: "", kind: "", durable: "", lifecycle: "", residency: "" };
    this.cursor = "";
    this.requestGeneration = 0;
    this.render();
    this.load();
  }
  set system(value) {
    if (this._system === value) return;
    this._system = value;
    this.selected = "";
    this.cursor = "";
    this.requestGeneration++;
    if (this.isConnected) {
      this.list.replaceChildren();
      this.more.disabled = true;
      this.message.textContent = value ? "Loading actors…" : "Choose a system.";
      if (value) this.load();
    }
  }
  get system() { return this._system; }

  render() {
    this.replaceChildren(el("aside", {},
      el("div", { class: "directory-heading toolbar" }, el("h2", {}, "Actors"),
        el("button", { class: "quiet-action", onclick: () => { this.cursor = ""; this.load(); }, "aria-label": "Refresh actor directory" }, "Refresh")),
      el("form", { class: "filters", onsubmit: event => { event.preventDefault(); this.cursor = ""; this.load(); } },
        this.input("prefix", "Actor ID", "Actor ID prefix"),
        el("details", { class: "filter-disclosure" }, el("summary", {}, "More filters"),
          el("div", { class: "filter-grid" }, this.input("kind", "Kind", "Any kind"),
            this.select("durable", ["", "true", "false"]), this.select("lifecycle", ["", "active", "terminal"]),
            this.select("residency", ["", "resident", "paged out", "hydrating"]))),
        el("button", { type: "submit" }, "Apply filters")),
      this.message = el("p", { class: "muted" }, "Choose a system."),
      this.list = el("ul", { class: "directory" }),
      this.more = el("button", { class: "load-more", onclick: () => this.load(true), disabled: "" }, "Load more")));
  }

  input(name, label, placeholder) {
    const input = el("input", { type: name === "prefix" ? "search" : "text", placeholder, "aria-label": name, oninput: event => { this.filters[name] = event.target.value.trim(); } });
    return el("label", {}, el("span", {}, label), input);
  }

  select(name, values) {
    const select = el("select", { "aria-label": name, onchange: event => { this.filters[name] = event.target.value; } },
      ...values.map(value => el("option", { value }, value || `Any ${name}`)));
    return el("label", {}, el("span", {}, name[0].toUpperCase() + name.slice(1)), select);
  }

  async load(append = false) {
    if (!this.system) return;
    const generation = ++this.requestGeneration;
    const system = this.system;
    const cursor = append ? this.cursor : "";
    this.more.disabled = true;
    this.message.className = "muted";
    this.message.textContent = "Loading actors…";
    try {
      const page = await api(actorQuery(system, this.filters, cursor));
      if (generation !== this.requestGeneration || system !== this.system) return;
      const items = get(page, "actors") || get(page, "items") || [];
      if (!append) this.list.replaceChildren();
      const present = new Set([...this.list.querySelectorAll("button[data-actor-id]")].map(button => button.dataset.actorId));
      for (const actor of items) {
        const id = String(get(actor, "id"));
        if (present.has(id)) continue;
        present.add(id);
        const residency = get(actor, "residency") || "unknown";
        const button = el("button", { title: id, "aria-current": String(id === this.selected), onclick: () => {
          this.selected = id;
          this.dispatchEvent(new CustomEvent("actor-select", { detail: id, bubbles: true }));
          this.loadSelection();
        } }, el("span", { class: "actor-primary" }, el("strong", { class: "actor-id" }, compact(id)),
          el("span", { class: "actor-state", "data-state": residency }, residency)),
        el("span", { class: "actor-meta" }, `${get(actor, "kind") || "unknown"}${get(actor, "durable") ? " · durable" : ""}`));
        button.dataset.actorId = id;
        this.list.append(el("li", {}, button));
      }
      this.cursor = get(page, "next") || get(page, "nextCursor") || "";
      this.more.toggleAttribute("disabled", !this.cursor);
      const count = this.list.children.length;
      this.message.textContent = items.length ? `${count} ${count === 1 ? "actor" : "actors"}` : "No actors match.";
    } catch (error) {
      if (generation !== this.requestGeneration || system !== this.system) return;
      this.message.textContent = error.message;
      this.message.className = "error";
    }
  }

  loadSelection() {
    for (const button of this.list.querySelectorAll("button[data-actor-id]")) {
      button.setAttribute("aria-current", String(button.dataset.actorId === this.selected));
    }
  }
}
customElements.define("actor-directory", ActorDirectory);

class DefinitionView extends HTMLElement {
  set data(value) {
    const signature = [get(value, "pinnedRevision"), get(value, "currentRevision"), get(value, "currentAvailable")].join("\u0000");
    this._data = value;
    if (signature !== this._signature) {
      this._signature = signature;
      this.render();
    } else {
      this.updateRuntime();
    }
  }
  set active(value) { this._active = value || new Set(); this.updateRuntime(); }
  get active() { return this._active || new Set(); }
  set selectedTransitions(value) { this._selectedTransitions = value || new Set(); this.updateRuntime(); }

  render() {
    this.replaceChildren();
    if (!this._data) return;
    const pinned = get(this._data, "pinned");
    const currentAvailable = Boolean(get(this._data, "currentAvailable"));
    const sameRevision = currentAvailable && get(this._data, "pinnedRevision") === get(this._data, "currentRevision");
    this.append(el("h2", {}, "Definition"), el("dl", { class: "facts revision-facts" },
      ...field("Pinned revision", copyableValue(get(this._data, "pinnedRevision"), "pinned revision")),
      ...field("Current revision", sameRevision ? "same as pinned" : currentAvailable ? copyableValue(get(this._data, "currentRevision"), "current revision") : "unavailable")));
    this.search = el("input", { type: "search", placeholder: "State, event, condition, or action", "aria-label": "Search definition",
      oninput: event => this.applySearch(event.target.value) });
    this.searchStatus = el("span", { class: "muted search-status", "aria-live": "polite" });
    this.append(el("label", { class: "definition-search" }, el("span", {}, "Find behavior"), this.search, this.searchStatus));
    if (pinned) this.append(this.definitionSection(sameRevision ? "Pinned · current" : "Pinned", pinned, true, get(this._data, "pinnedSource")));
    if (sameRevision) this.append(el("p", { class: "muted same-definition" }, "Current is the same as pinned."));
    else if (currentAvailable) this.append(this.definitionSection("Current", get(this._data, "current"), false, get(this._data, "currentSource")));
    else this.append(el("section", {}, el("h3", {}, "Current"), el("p", { class: "muted" }, "Current definition is unavailable.")));
    this.updateRuntime();
  }

  definitionSection(label, definition, pinned, source) {
    const chartID = String(get(definition, "id") || "");
    const section = el("section", { class: "definition-tree", "data-revision-role": pinned ? "pinned" : "current" },
      el("h3", {}, label),
      el("dl", { class: "facts definition-facts" },
        ...field("Chart", chartID || "—"), ...field("Name", get(definition, "name") || "—"),
        ...field("Datamodel", get(definition, "datamodel") || "—"),
        ...field("Data binding", get(definition, "dataBinding") || "early")));
    if ((get(definition, "data") || []).length) section.append(this.dataDefinitions("Chart data", get(definition, "data")));
    section.append(this.state(get(definition, "root"), pinned, chartID, true));
    const sourceValue = source || definition;
    section.append(el("details", { class: "definition-source" }, el("summary", {}, "Complete definition JSON"),
      el("pre", { class: "code" }, JSON.stringify(sourceValue, null, 2))));
    return section;
  }

  state(state, pinned, chartID, root = false) {
    if (!state) return el("p", { class: "muted" }, "Unavailable");
    const id = stateID(state);
    const transitions = get(state, "transitions") || [];
    const selected = transitions.some((_, index) => pinned && this._selectedTransitions?.has(`${id}\u0000${index}`));
    const active = this.active.has(id);
    const node = el("details", { class: `state state-def${active ? " active" : ""}`, "data-state-id": id,
      "data-search": JSON.stringify(state).toLocaleLowerCase(), ...(root || active || selected ? { open: "" } : {}) },
      el("summary", { class: "state-summary" },
        el("span", { class: "state-status", "aria-label": active ? "active state" : "inactive state" }, active ? "●" : "○"),
        el("strong", {}, id), el("span", { class: "behavior-meta" }, stateKind(get(state, "kind")))));
    const body = el("div", { class: "state-body" });
    if (stateKind(get(state, "kind")) === "history") {
      body.append(el("p", { class: "behavior-line" }, `History: ${Number(get(state, "history")) === 1 ? "deep" : "shallow"}`));
    }
    if (get(state, "initial")) body.append(this.transition(get(state, "initial"), "initial", id, pinned, chartID, true));
    body.append(this.renderBlocks(get(state, "onEntry"), "Entry actions", chartID));
    body.append(this.renderBlocks(get(state, "onExit"), "Exit actions", chartID));
    (get(state, "transitions") || []).forEach((transition, index) => {
      body.append(this.transition(transition, index, id, pinned, chartID));
    });
    if ((get(state, "data") || []).length) body.append(this.dataDefinitions("State data", get(state, "data")));
    if ((get(state, "invokes") || []).length) body.append(this.invocations(get(state, "invokes"), chartID));
    if (get(state, "doneData")) body.append(this.doneData(get(state, "doneData")));
    const children = get(state, "children") || [];
    if (children.length) {
      const childList = el("div", { class: "child-states" }, el("h4", {}, `Child states · ${children.length}`));
      for (const child of children) childList.append(this.state(child, pinned, chartID));
      body.append(childList);
    }
    node.append(body);
    node.dataset.search = `${JSON.stringify(state)} ${node.textContent}`.toLocaleLowerCase();
    return node;
  }

  transition(transition, index, state, pinned, chartID, initial = false) {
    const key = `${state}\u0000${index}`;
    const selected = pinned && this._selectedTransitions?.has(key);
    const events = get(transition, "events") || [];
    const targets = get(transition, "targets") || [];
    const type = get(transition, "type") || "external";
    const trigger = initial ? "initial" : events.length ? `on ${events.join(", ")}` : "eventless";
    const destination = targets.length ? `→ ${targets.join(", ")}` : "targetless";
    const node = el("details", { class: `transition${selected ? " selected" : ""}`, "data-transition-key": key,
      "data-transition-state": state, "data-transition-index": String(index),
      "data-search": JSON.stringify(transition).toLocaleLowerCase(), ...(selected ? { open: "" } : {}) },
      el("summary", { tabindex: "-1" }, el("span", { class: "selection-marker" }, selected ? "▶" : "○"),
        el("strong", {}, trigger), el("span", { class: "behavior-meta" }, `${destination} · ${type}${initial ? "" : ` · ${state}[${index}]`}`)));
    const body = el("div", { class: "transition-body" });
    if (get(transition, "condition")) body.append(this.expression("Condition", get(transition, "condition")));
    body.append(this.renderBlocks(get(transition, "actions"), "Actions", chartID));
    if (!body.children.length) body.append(el("p", { class: "muted" }, "No condition or actions."));
    node.append(body);
    node.dataset.search = `${JSON.stringify(transition)} ${node.textContent}`.toLocaleLowerCase();
    return node;
  }

  renderBlocks(blocks, label, chartID) {
    if (!(blocks || []).length) return document.createDocumentFragment();
    const group = el("div", { class: "behavior-group" }, el("h4", {}, label));
    blocks.forEach((block, blockIndex) => {
      const list = el("ol", { class: "action-list", ...(blocks.length > 1 ? { "aria-label": `${label} block ${blockIndex + 1}` } : {}) });
      for (const action of block || []) list.append(el("li", {}, this.executable(action, chartID)));
      if (!list.children.length) list.append(el("li", { class: "muted" }, "Empty block"));
      group.append(list);
    });
    group.dataset.search = group.textContent.toLocaleLowerCase();
    return group;
  }

  executable(action, chartID) {
    const kind = String(get(action, "kind") || "unknown");
    const value = get(action, kind);
    if (kind === "call") {
      const fn = get(value, "function") || {};
      const fullName = String(get(fn, "name") || "(unnamed)");
      const name = chartID && fullName.startsWith(`${chartID}.`) ? fullName.slice(chartID.length + 1) : fullName;
      const version = get(fn, "version");
      const node = el("div", { class: "action call", title: fullName }, el("strong", {}, `call ${name}${version ? `@${version}` : ""}`));
      const args = get(fn, "args") || [];
      args.forEach((argument, index) => node.append(this.expression(`Argument ${index + 1}`, argument)));
      return node;
    }
    if (kind === "raise") {
      const node = this.actionDetails(`raise ${get(value, "event") || "dynamic event"}`, kind);
      this.appendExpressions(node, value, [["eventExpr", "Event"], ["data", "Data"]]);
      return node;
    }
    if (kind === "send") {
      const event = get(value, "event") || "dynamic event";
      const target = get(value, "target") || (get(value, "targetExpr") ? "dynamic target" : "default target");
      const node = this.actionDetails(`send ${event} → ${target}`, kind);
      for (const [name, label] of [["type", "Processor"], ["id", "Send ID"], ["delay", "Delay"]]) {
        if (get(value, name)) node.append(el("p", { class: "behavior-line" }, `${label}: ${get(value, name)}`));
      }
      this.appendExpressions(node, value, [["eventExpr", "Event"], ["targetExpr", "Target"], ["typeExpr", "Processor"],
        ["idLocation", "ID location"], ["delayExpr", "Delay"], ["content", "Content"]]);
      if ((get(value, "params") || []).length) node.append(this.params(get(value, "params")));
      return node;
    }
    if (kind === "cancel") {
      const node = this.actionDetails(`cancel ${get(value, "sendID") || "dynamic send"}`, kind);
      this.appendExpressions(node, value, [["sendIDExpr", "Send ID"]]);
      return node;
    }
    if (kind === "log") {
      const node = this.actionDetails(`log${get(value, "label") ? ` ${get(value, "label")}` : ""}`, kind);
      this.appendExpressions(node, value, [["labelExpr", "Label"], ["expr", "Value"]]);
      return node;
    }
    if (kind === "assign") {
      const node = this.actionDetails("assign", kind);
      this.appendExpressions(node, value, [["location", "Location"], ["expr", "Value"]]);
      return node;
    }
    if (kind === "choose") {
      const branches = get(value, "branches") || [];
      const node = this.actionDetails(`choose · ${branches.length} branch${branches.length === 1 ? "" : "es"}`, kind);
      branches.forEach((branch, index) => {
        const branchNode = el("details", { class: "action-branch" }, el("summary", {}, `Branch ${index + 1}`),
          this.expression("Condition", get(branch, "condition")), this.renderBlocks(get(branch, "actions"), "Actions", chartID));
        node.append(branchNode);
      });
      if ((get(value, "else") || []).length) node.append(el("div", { class: "action-branch" }, el("strong", {}, "Else"), this.renderBlocks(get(value, "else"), "Actions", chartID)));
      return node;
    }
    if (kind === "foreach") {
      const item = get(value, "item") || "item";
      const index = get(value, "index");
      const node = this.actionDetails(`foreach ${item}${index ? `, ${index}` : ""}`, kind);
      node.append(this.expression("Array", get(value, "array")), this.renderBlocks(get(value, "actions"), "Actions", chartID));
      return node;
    }
    if (kind === "script") {
      const node = this.actionDetails("script", kind);
      node.append(this.expression("Script", get(value, "expr")));
      return node;
    }
    if (kind === "extension") {
      const node = this.actionDetails(`extension ${get(value, "namespace") || "?"}:${get(value, "name") || "?"}`, kind);
      const data = document.createElement("canonical-value");
      data.value = get(value, "data");
      node.append(data);
      return node;
    }
    return el("details", { class: `action ${kind}` }, el("summary", {}, kind), el("pre", { class: "code" }, JSON.stringify(action, null, 2)));
  }

  actionDetails(summary, kind) {
    return el("details", { class: `action ${kind}` }, el("summary", {}, summary));
  }

  appendExpressions(node, object, fields) {
    for (const [name, label] of fields) if (get(object, name)) node.append(this.expression(label, get(object, name)));
  }

  expression(label, expression) {
    if (!expression) return el("span", { class: "muted" }, `${label}: —`);
    const kind = get(expression, "kind") || "unknown";
    const data = get(expression, "data");
    const scalar = ["string", "number", "bool"].includes(get(data, "kind")) ? ` · ${get(data, get(data, "kind"))}` : "";
    const canonical = document.createElement("canonical-value");
    canonical.value = data;
    return el("details", { class: "expression" }, el("summary", {}, `${label}: ${kind}${scalar}`), canonical);
  }

  dataExpression(label, expression) {
    if (!expression) return el("span", { class: "muted" }, "—");
    const kind = get(expression, "kind") || "unknown";
    const data = get(expression, "data");
    const dataKind = get(data, "kind");
    if (["null", "bool", "number", "string"].includes(dataKind)) {
      const scalar = dataKind === "null" ? "null" : String(get(data, dataKind));
      return el("span", { class: "data-expression", title: `${label}: ${kind}` },
        el("span", { class: "expression-kind" }, kind), el("code", { class: "data-scalar" }, scalar));
    }
    const canonical = document.createElement("canonical-value");
    canonical.value = data;
    return el("details", { class: "data-expression" },
      el("summary", {}, el("span", { class: "expression-kind" }, kind), el("span", { class: "data-scalar" }, dataKind || "value")), canonical);
  }

  params(values) {
    const group = el("div", { class: "behavior-group" }, el("h5", {}, "Parameters"));
    for (const parameter of values || []) {
      const row = el("div", { class: "parameter" }, el("strong", {}, get(parameter, "name") || "(unnamed)"));
      if (get(parameter, "expr")) row.append(this.expression("Value", get(parameter, "expr")));
      if (get(parameter, "location")) row.append(this.expression("Location", get(parameter, "location")));
      group.append(row);
    }
    return group;
  }

  dataDefinitions(label, values) {
    const group = el("details", { class: "state-behavior data-definitions" },
      el("summary", {}, el("span", {}, label), el("span", { class: "data-count" }, values.length)));
    const table = el("div", { class: "data-table", role: "table", "aria-label": label },
      el("div", { class: "data-table-header", role: "row" },
        el("span", { role: "columnheader" }, "Variable"), el("span", { role: "columnheader" }, "Initial value")));
    for (const definition of values) {
      const values = el("div", { class: "data-initializers", role: "cell" });
      if (get(definition, "source")) values.append(el("span", { class: "data-source" }, get(definition, "source")));
      if (get(definition, "expr")) values.append(this.dataExpression("Initial value", get(definition, "expr")));
      if (get(definition, "content")) values.append(this.dataExpression("Content", get(definition, "content")));
      if (!values.children.length) values.append(el("span", { class: "muted" }, "—"));
      table.append(el("div", { class: "data-definition", role: "row" },
        el("strong", { class: "data-name", role: "cell" }, get(definition, "id") || "(unnamed)"), values));
    }
    group.append(table);
    group.dataset.search = group.textContent.toLocaleLowerCase();
    return group;
  }

  invocations(values, chartID) {
    const group = el("details", { class: "state-behavior" }, el("summary", {}, `Invocations · ${values.length}`));
    values.forEach((invoke, index) => {
      const identity = get(invoke, "id") || get(invoke, "definitionId") || get(invoke, "src") || `Invocation ${index + 1}`;
      const node = el("details", { class: "invocation" }, el("summary", {}, identity));
      for (const [name, label] of [["definitionId", "Definition"], ["type", "Type"], ["src", "Source"]]) {
        if (get(invoke, name)) node.append(el("p", { class: "behavior-line" }, `${label}: ${get(invoke, name)}`));
      }
      if (get(invoke, "autoForward")) node.append(el("p", { class: "behavior-line" }, "Autoforward: enabled"));
      this.appendExpressions(node, invoke, [["idLocation", "ID location"], ["typeExpr", "Type"], ["srcExpr", "Source"], ["content", "Content"]]);
      if ((get(invoke, "params") || []).length) node.append(this.params(get(invoke, "params")));
      node.append(this.renderBlocks(get(invoke, "finalize"), "Finalize actions", chartID));
      group.append(node);
    });
    group.dataset.search = group.textContent.toLocaleLowerCase();
    return group;
  }

  doneData(value) {
    const node = el("details", { class: "state-behavior" }, el("summary", {}, "Done data"));
    if ((get(value, "params") || []).length) node.append(this.params(get(value, "params")));
    if (get(value, "content")) node.append(this.expression("Content", get(value, "content")));
    node.dataset.search = node.textContent.toLocaleLowerCase();
    return node;
  }

  applySearch(value) {
    const query = String(value || "").trim().toLocaleLowerCase();
    let matches = 0;
    for (const transition of this.querySelectorAll(".transition")) {
      transition.hidden = Boolean(query) && !transition.dataset.search.includes(query);
      if (query && !transition.hidden) {
        matches++;
        transition.open = true;
      }
    }
    for (const behavior of this.querySelectorAll(".state-body > .behavior-group, .state-body > .state-behavior")) {
      behavior.hidden = Boolean(query) && !behavior.dataset.search.includes(query);
    }
    for (const state of [...this.querySelectorAll(".state-def")].reverse()) {
      state.hidden = Boolean(query) && !state.dataset.search.includes(query);
      if (query && !state.hidden) state.open = true;
    }
    if (this.searchStatus) this.searchStatus.textContent = query ? `${matches} matching transition${matches === 1 ? "" : "s"}` : "";
  }

  updateRuntime() {
    if (!this.isConnected && !this.children.length) return;
    for (const state of this.querySelectorAll(".state-def")) {
      const active = this.active.has(state.dataset.stateId);
      state.classList.toggle("active", active);
      const marker = state.querySelector(":scope > .state-summary > .state-status");
      if (marker) {
        marker.textContent = active ? "●" : "○";
        marker.setAttribute("aria-label", active ? "active state" : "inactive state");
      }
      if (active) state.open = true;
    }
    for (const transition of this.querySelectorAll(".transition")) {
      const selected = transition.closest(".definition-tree")?.dataset.revisionRole === "pinned" && this._selectedTransitions?.has(transition.dataset.transitionKey);
      transition.classList.toggle("selected", Boolean(selected));
      const marker = transition.querySelector(":scope > summary > .selection-marker");
      if (marker) marker.textContent = selected ? "▶" : "○";
      if (selected) {
        transition.open = true;
        for (let parent = transition.parentElement; parent; parent = parent.parentElement) if (parent.matches?.("details.state-def")) parent.open = true;
      }
    }
  }

  focusTransition(state, index) {
    const transition = [...this.querySelectorAll('.definition-tree[data-revision-role="pinned"] .transition')]
      .find(node => node.dataset.transitionState === String(state) && node.dataset.transitionIndex === String(index));
    if (!transition) return false;
    this.closest("details.disclosure")?.setAttribute("open", "");
    transition.hidden = false;
    transition.open = true;
    for (let parent = transition.parentElement; parent; parent = parent.parentElement) if (parent.matches?.("details.state-def")) parent.open = true;
    const summary = transition.querySelector(":scope > summary");
    summary?.focus({ preventScroll: true });
    (summary || transition).scrollIntoView({ block: "center", behavior: "smooth" });
    return true;
  }
}
customElements.define("definition-view", DefinitionView);

class EventForm extends HTMLElement {
  connectedCallback() {
    if (this.form) return;
    this.value = canonicalValue();
    this.suggestionListID = `inspector-event-names-${++eventSuggestionSequence}`;
    this.render();
  }
  set target(value) { this._target = value; }
  set suggestions(value) {
    this._suggestions = [...new Set(value || [])];
    this.updateSuggestions();
  }

  updateSuggestions() {
    if (!this.suggestionList) return;
    this.suggestionList.replaceChildren(...(this._suggestions || []).map(name => el("option", { value: name })));
    this.name.placeholder = this._suggestions?.length ? "Choose or enter an event" : "Enter an event name";
  }

  render() {
    this.editor = document.createElement("value-editor");
    this.editor.value = this.value;
    this.submitButton = el("button", { type: "submit" }, "Send once");
    this.suggestionList = el("datalist", { id: this.suggestionListID });
    this.replaceChildren(el("div", { class: "section-heading" }, el("div", {}, el("p", { class: "eyebrow" }, "Command"),
      el("h2", {}, "Send external event"))),
    this.form = el("form", { onsubmit: event => this.submit(event) },
      el("label", {}, "Event name ", this.name = el("input", { required: "", pattern: "[^\\s]+", list: this.suggestionListID, "aria-label": "Event name" })),
      this.suggestionList,
      el("fieldset", { class: "event-data" }, el("legend", {}, "Event data"), this.editor),
      this.submitButton, this.message = el("span", { class: "muted", "aria-live": "polite" })));
    this.updateSuggestions();
  }

  async submit(event) {
    event.preventDefault();
    if (this.inFlight) return;
    this.inFlight = true;
    this.submitButton.disabled = true;
    this.message.className = "muted";
    this.message.textContent = "Sending one request…";
    try {
      await api(`v1/events?${new URLSearchParams(this._target)}`, { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: this.name.value, data: this.editor.value }) });
      this.message.className = "muted";
      this.message.textContent = "Accepted. Not retried.";
    } catch (error) {
      this.message.className = "error";
      this.message.textContent = `Not sent: ${error.message}`;
    } finally {
      this.inFlight = false;
      this.submitButton.disabled = false;
    }
  }
}
customElements.define("event-form", EventForm);

class InspectorApp extends HTMLElement {
  connectedCallback() {
    this.timeline = [];
    this.seenSequences = new Map();
    this.selectionVersion = 0;
    this.systemGeneration = 0;
    this.connectionGeneration = 0;
    clearInterval(this.relativeTimer);
    this.relativeTimer = setInterval(() => this.updateRelativeTimes(), 1000);
    this.render();
    this.loadSystems();
  }

  render() {
    this.replaceChildren(el("header", { class: "app-header" },
      el("div", { class: "brand" }, el("span", { class: "brand-mark", "aria-hidden": "true" }, "S"),
        el("div", {}, el("h1", {}, "Inspector"), el("p", {}, "Runtime observability"))),
      el("div", { class: "header-controls" },
        el("label", { class: "system-picker" }, el("span", {}, "System"), this.picker = el("select", { "aria-label": "System", onchange: event => this.selectSystem(event.target.value) })),
        this.connection = el("span", { class: "status", role: "status", "aria-live": "polite", "data-state": "disconnected", "data-short": "Offline" }, "Offline"),
        el("button", { class: "retry-stream", onclick: () => this.connect() }, "Reconnect"))),
    el("div", { class: "layout" }, this.directory = document.createElement("actor-directory"),
      this.main = el("main", {}, el("div", { class: "empty" }, "Select an actor to inspect."))));
    this.directory.addEventListener("actor-select", event => this.selectActor(event.detail));
  }

  async loadSystems() {
    try {
      const data = await api("v1/systems");
      const systems = get(data, "systems") || [];
      this.picker.replaceChildren(el("option", { value: "" }, "Choose…"), ...systems.map(system => el("option", { value: system }, system)));
      if (systems.length === 1) { this.picker.value = systems[0]; this.selectSystem(systems[0]); }
    } catch (error) { this.main.replaceChildren(el("p", { class: "error" }, error.message)); }
  }

  selectSystem(system) {
    this.selectionVersion++;
    this.systemGeneration++;
    this.system = system;
    this.actor = "";
    this.latestTrace = null;
    this.latestTraceStreamSequence = -1;
    this.timeline = [];
    this.seenSequences.clear();
    this.eventForm = null;
    this.definitionView = null;
    clearTimeout(this.refreshTimer);
    clearTimeout(this.directoryTimer);
    this.directory.system = system;
    this.classList.remove("detail-active");
    this.connect();
    this.main.replaceChildren(el("div", { class: "empty" }, "Select an actor to inspect."));
  }

  setConnectionStatus(text, state, short) {
    this.connection.textContent = text;
    this.connection.dataset.state = state;
    this.connection.dataset.short = short;
  }

  connect() {
    this.source?.close();
    if (!this.system) return;
    const version = this.systemGeneration;
    const connection = ++this.connectionGeneration;
    const current = () => version === this.systemGeneration && connection === this.connectionGeneration;
    this.setConnectionStatus("Stream connecting…", "disconnected", "Connecting…");
    this.source = new EventSource(`v1/stream?${new URLSearchParams({ system: this.system })}`);
    this.source.onopen = () => { if (current()) this.setConnectionStatus("Live", "connected", "Live"); };
    this.source.onerror = () => { if (current()) this.setConnectionStatus("Offline · data retained", "disconnected", "Offline"); };
    this.source.addEventListener("gap", event => { if (current()) this.onGap(JSON.parse(event.data)); });
    this.source.addEventListener("observation", event => { if (current()) this.onObservation(JSON.parse(event.data)); });
  }

  rememberSequence(record) {
    const sequence = Number(get(record, "sequence"));
    if (!Number.isSafeInteger(sequence) || sequence < 0) return true;
    if (this.seenSequences.has(sequence)) return false;
    this.seenSequences.set(sequence, true);
    while (this.seenSequences.size > SEEN_SEQUENCE_LIMIT) this.seenSequences.delete(this.seenSequences.keys().next().value);
    return true;
  }

  pushTimeline(entry) {
    this.timeline.push(entry);
    this.timeline.sort((a, b) => (a.sequence ?? Number.MAX_SAFE_INTEGER) - (b.sequence ?? Number.MAX_SAFE_INTEGER));
    if (this.timeline.length > TIMELINE_LIMIT) this.timeline.splice(0, this.timeline.length - TIMELINE_LIMIT);
  }

  onGap(record) {
    if (!this.actor) {
      this.setConnectionStatus("Stream gap — state refresh scheduled", "gap", "Stream gap");
      this.scheduleDirectoryRefresh();
      return;
    }
    if (!this.rememberSequence(record)) return;
    const streamSequence = Number(get(record, "sequence"));
    if (streamSequence >= (this.latestTraceStreamSequence ?? -1)) {
      this.latestTrace = null;
      this.latestTraceStreamSequence = streamSequence;
    }
    const reason = get(record, "reason") || "observations lost";
    const dropped = Number(get(record, "dropped") || 0);
    this.setConnectionStatus("Stream gap — state refresh scheduled", "gap", "Stream gap");
    this.pushTimeline({ class: "gap", category: "gap", sequence: streamSequence,
      timestamp: get(record, "timestamp") || new Date().toISOString(), text: `stream gap · ${reason}${dropped ? ` · ${dropped} dropped` : ""}` });
    this.scheduleDirectoryRefresh();
    if (this.actor) this.scheduleRefresh();
  }

  onObservation(record) {
    const observation = get(record, "observation");
    const actor = get(observation, "actor");
    if (!observation) return;
    const kind = get(observation, "kind") || "observation";
    if (["actor.discovered", "actor.terminal", "residency.changed"].includes(kind)) this.scheduleDirectoryRefresh();
    if (kind === "definition.published" && this.actor) this.scheduleRefresh();
    if (String(get(actor, "id") || "") !== this.actor) return;
    if (!this.rememberSequence(record)) return;
    const macrostep = get(observation, "macrostep");
    const streamSequence = Number(get(record, "sequence"));
    if (macrostep && (!this.latestTrace || streamSequence >= (this.latestTraceStreamSequence ?? -1))) {
      this.latestTrace = macrostep;
      this.latestTraceStreamSequence = streamSequence;
    }
    const timestamp = get(macrostep, "timestamp") || get(observation, "timestamp") || get(record, "timestamp") || new Date().toISOString();
    this.pushTimeline({ class: "live", category: observationCategory(kind), sequence: streamSequence, timestamp,
      text: observationSummary(kind, actor, macrostep), transitions: macrostep ? macrostepTransitionRefs(macrostep) : [] });
    this.scheduleRefresh();
  }

  async selectActor(id) {
    this.selectionVersion++;
    this.actor = id;
    this.latestTrace = null;
    this.latestTraceStreamSequence = -1;
    this.timeline = [];
    this.seenSequences.clear();
    this.eventForm = null;
    this.definitionView = null;
    this.classList.add("detail-active");
    clearTimeout(this.refreshTimer);
    const version = this.selectionVersion;
    const system = this.system;
    await Promise.all([this.loadRecent(version, system, id), this.refresh(true)]);
  }

  async loadRecent(version, system, actorID) {
    try {
      let cursor = 0;
      let boundary;
      for (;;) {
        const page = await api(`v1/recent?${new URLSearchParams({ system, cursor: String(cursor), limit: String(RECENT_PAGE_LIMIT) })}`);
        if (version !== this.selectionVersion || system !== this.system || actorID !== this.actor) return;
        if (boundary === undefined) boundary = Number(get(page, "latest") || 0);
        const records = get(page, "records") || [];
        records.sort((a, b) => Number(get(a, "sequence") || 0) - Number(get(b, "sequence") || 0));
        if (get(page, "expired") && !records.some(record => get(record, "kind") === "gap")) {
          this.pushTimeline({ class: "gap", category: "gap", timestamp: new Date().toISOString(), text: "stream gap · retained live observations expired before catch-up" });
        }
        for (const record of records) {
          if (get(record, "kind") === "gap") this.onGap(record);
          else this.onObservation(record);
        }
        const next = Number(get(page, "next") || cursor);
        if (next >= boundary || next <= cursor) break;
        cursor = next;
      }
      this.scheduleRefresh();
    } catch (error) {
      if (version === this.selectionVersion && system === this.system && actorID === this.actor) {
        this.pushTimeline({ class: "gap", category: "gap", timestamp: new Date().toISOString(), text: `recent lossy trace unavailable · ${error.message}` });
        this.scheduleRefresh();
      }
    }
  }

  scheduleDirectoryRefresh() {
    if (this.directoryTimer) return;
    this.directoryTimer = setTimeout(() => { this.directoryTimer = null; this.directory.load(); }, 300);
  }

  scheduleRefresh() {
    if (this.displayPaused) return;
    if (this.refreshTimer) return;
    this.refreshTimer = setTimeout(() => { this.refreshTimer = null; this.refresh(false); }, 300);
  }

  transitionSet() {
    const selected = new Set();
    for (const step of get(this.latestTrace, "microsteps") || []) {
      for (const transition of get(step, "transitions") || []) selected.add(`${get(transition, "source")}\u0000${get(transition, "index")}`);
    }
    return selected;
  }

  async refresh(showLoading = true) {
    if (!this.system || !this.actor) return;
    if (this.refreshActive) {
      this.refreshQueued = true;
      return;
    }
    this.refreshActive = true;
    const version = this.selectionVersion;
    const refreshVersion = (this.refreshVersion || 0) + 1;
    this.refreshVersion = refreshVersion;
    const system = this.system;
    const actorID = this.actor;
    if (showLoading && !this.main.querySelector(".detail-shell")) this.main.replaceChildren(el("p", {}, "Loading actor…"));
    const query = new URLSearchParams({ system, id: actorID });
    try {
      const [actor, definition, history] = await Promise.all([
        api(`v1/actor?${query}`), api(`v1/definition?${query}`),
        api(`v1/history?${query}&tail=true&limit=100`).catch(() => null),
      ]);
      if (version !== this.selectionVersion || refreshVersion !== this.refreshVersion || system !== this.system || actorID !== this.actor) return;
      this.draw(actor, definition, history);
    } catch (error) {
      if (version !== this.selectionVersion || refreshVersion !== this.refreshVersion) return;
      this.main.replaceChildren(el("p", { class: "error" }, error.message), el("button", { onclick: () => this.refresh() }, "Retry"));
    } finally {
      this.refreshActive = false;
      if (this.refreshQueued) {
        this.refreshQueued = false;
        this.scheduleRefresh();
      }
    }
  }

  valuePanel(label, value) {
    const component = document.createElement("canonical-value");
    component.value = value;
    return [el("h3", {}, label), el("div", { class: "dense-panel" }, component)];
  }

  eventList(label, events) {
    const list = el("ol", { class: "event-list dense-panel" });
    for (const event of events || []) {
      const data = document.createElement("canonical-value");
      data.value = get(event, "data");
      list.append(el("li", {}, el("strong", {}, get(event, "name") || "(unnamed event)"),
        el("span", { class: "muted" }, ` · type ${get(event, "type") ?? "—"}`), data));
    }
    if (!list.children.length) list.append(el("li", { class: "muted" }, "Empty"));
    return [el("h3", {}, label), list];
  }

  timelineItem(item) {
    const time = el("time", { datetime: item.timestamp, "data-relative-time": item.timestamp }, relativeSeconds(item.timestamp));
    const row = el("li", { class: item.class }, time, ` · ${item.text}`);
    if (item.transitions?.length) {
      row.append(` · ${item.transitions.length === 1 ? "transition" : `${item.transitions.length} transitions:`} `);
      item.transitions.slice(0, 3).forEach((ref, index) => {
        if (index) row.append(", ");
        const key = `${ref.source}\u0000${ref.index}`;
        row.append(el("button", { type: "button", class: "transition-ref", "data-transition-key": key,
          "data-transition-state": ref.source, "data-transition-index": String(ref.index),
          title: `Show transition ${ref.source}[${ref.index}] in the pinned definition`,
          onclick: () => this.definitionView?.focusTransition(ref.source, ref.index) }, `${ref.source}[${ref.index}]`));
      });
      if (item.transitions.length > 3) row.append(", …");
    } else if (item.category === "macrostep") {
      row.append(" · no transition");
    }
    return row;
  }

  updateRelativeTimes() {
    for (const node of this.main?.querySelectorAll("time[data-relative-time]") || []) {
      node.textContent = relativeSeconds(node.dataset.relativeTime);
    }
  }

  draw(actor, definition, history) {
    const info = get(actor, "info");
    const live = get(actor, "live");
    const fullID = String(get(info, "id") ?? "—");
    const address = String(get(info, "address") ?? "");
    const facts = el("dl", { class: "facts actor-facts" }, ...field("ID", copyableValue(fullID, "actor ID")),
      ...field("Address", copyableValue(get(info, "address"), "actor address")),
      ...field("Kind", get(info, "kind")), ...field("Revision", copyableValue(get(info, "revision"), "revision")), ...field("Lifecycle", get(info, "lifecycle")),
      ...field("Residency", get(info, "residency")), ...field("Durable", String(get(info, "durable"))));
    const metadata = el("details", { class: "actor-metadata" }, el("summary", {}, "Identity and revision"), facts);
    const title = el("div", { class: "actor-title" }, el("p", { class: "eyebrow" }, "Actor"), el("h2", { title: fullID }, fullID));
    if (address && address !== fullID) title.append(el("p", { class: "actor-address code" }, address));
    const summary = el("section", { class: "actor-summary" },
      el("div", { class: "actor-hero" }, title,
      el("button", { class: "quiet-action", onclick: () => { this.directory.load(); this.refresh(); }, "aria-label": "Refresh selected actor" }, "Refresh")),
      el("div", { class: "actor-badges" },
        el("span", { class: "actor-badge primary", "data-state": get(info, "lifecycle") }, get(info, "lifecycle") || "unknown lifecycle"),
        el("span", { class: "actor-badge", "data-state": get(info, "residency") }, get(info, "residency") || "unknown residency"),
        el("span", { class: "actor-badge" }, get(info, "kind") || "unknown kind"),
        el("span", { class: "actor-badge" }, get(info, "durable") ? "durable" : "ephemeral"),
        el("span", { class: `actor-badge live-indicator${live ? " available" : ""}` }, live ? "live state" : "metadata only")),
      metadata);
    const detail = el("section", { class: "live-detail" }, el("h2", {}, "Live actor detail"));
    if (live) {
      const configuration = get(live, "configuration") || [];
      detail.append(el("p", {}, `Active configuration: ${configuration.join(", ") || "empty"}`));
      detail.append(...this.valuePanel("Canonical datamodel", get(live, "datamodel")));
      detail.append(...this.eventList("Internal queue", get(live, "internalQueue")));
      detail.append(...this.eventList("External queue", get(live, "externalQueue")));
      for (const [key, label] of [["pendingSends", "Pending sends"], ["activeInvokes", "Active invokes"]]) {
        detail.append(el("h3", {}, label), el("pre", { class: "code dense-panel" }, JSON.stringify(get(live, key), null, 2) ?? "—"));
      }
    } else detail.append(el("p", { class: "muted" }, "Live state is unavailable because inspection does not hydrate paged-out actors. Activate the actor through application traffic to inspect its current datamodel and queues."));

    if (!this.definitionView) this.definitionView = document.createElement("definition-view");
    this.definitionView.active = new Set(live ? get(live, "configuration") || [] : []);
    this.definitionView.selectedTransitions = this.transitionSet();
    this.definitionView.data = definition;
    const entries = get(history, "entries") || [];
    const durable = entries.slice().reverse().map(entry => {
      const event = get(entry, "event") || {};
      const data = document.createElement("canonical-value");
      data.value = get(event, "data");
      const timestamp = get(entry, "timestamp");
      const eventName = get(event, "name");
      const eventSummary = eventName ? `${eventName} [${eventTypeName(get(event, "type"))}]` : get(entry, "kind") === "session_started" ? "session started" : "(no event)";
      return el("li", { class: "durable" },
        el("time", { datetime: timestamp, "data-relative-time": timestamp }, relativeSeconds(timestamp)),
        ` · ${eventSummary} · persisted ${get(entry, "kind")} #${get(entry, "seq")}`,
        data);
    });
    const filteredTimeline = this.timeline.filter(item => item.class === "gap" || !this.timelineFilter || this.timelineFilter === "all" || item.category === this.timelineFilter);
    if (!durable.length) durable.push(el("li", { class: "empty-state" }, "No persisted history"));
    const liveItems = filteredTimeline.slice().reverse().map(item => this.timelineItem(item));
    if (!liveItems.length) liveItems.push(el("li", { class: "empty-state" }, "No live observations yet"));
    const timeline = el("section", { class: "timeline-panel" }, el("div", { class: "section-heading" },
      el("div", {}, el("p", { class: "eyebrow" }, "Signals"), el("h2", {}, "Activity")),
      el("div", { class: "timeline-controls" },
      el("button", { class: "quiet-action", onclick: () => { this.displayPaused = !this.displayPaused; this.draw(actor, definition, history); if (!this.displayPaused) this.refresh(); } }, this.displayPaused ? "Resume" : "Pause"),
      el("select", { "aria-label": "Timeline event kind", onchange: event => { this.timelineFilter = event.target.value; this.draw(actor, definition, history); } },
        ...["all", "macrostep", "residency", "lifecycle", "gap"].map(value => el("option", { value, ...(value === (this.timelineFilter || "all") ? { selected: "" } : {}) }, value === "all" ? "All events" : value))))),
      el("p", { class: "timeline-note muted" }, `Persisted history and the newest ${TIMELINE_LIMIT} lossy live observations are shown separately.`),
      el("div", { class: "timeline-columns" },
        el("div", { class: "timeline-column" }, el("div", { class: "column-heading" }, el("h3", {}, "Durable history"), el("span", {}, `${entries.length} ${entries.length === 1 ? "record" : "records"}`)),
          el("ul", { class: "timeline durable-history" }, ...durable)),
        el("div", { class: "timeline-column" }, el("div", { class: "column-heading" }, el("h3", {}, "Live trace"), el("span", {}, `${filteredTimeline.length} ${filteredTimeline.length === 1 ? "record" : "records"}`)),
          el("ul", { class: "timeline live-history" }, ...liveItems))));
    if (!this.eventForm) this.eventForm = document.createElement("event-form");
    this.eventForm.target = { system: this.system, id: this.actor };
    this.eventForm.suggestions = understoodEvents(get(definition, "pinned"));
    const back = el("button", { class: "back", onclick: () => this.classList.remove("detail-active") }, "← Back to actors");
    const shell = this.main.querySelector(".detail-shell");
    if (shell && shell.contains(this.eventForm)) {
      // Replace display panels individually while leaving the recursive event
      // editor attached. This preserves draft DOM, focus and caret while every
      // other panel continues to catch up during live traffic.
      shell.querySelector(".detail-toolbar")?.replaceWith(el("div", { class: "detail-toolbar" }, back));
      shell.querySelector("section.actor-summary")?.replaceWith(summary);
      shell.querySelector("section.timeline-panel")?.replaceWith(timeline);
      shell.querySelector("details.disclosure .live-detail")?.replaceWith(detail);
    } else {
      const progressive = el("details", { class: "disclosure" }, el("summary", {}, "Definition, datamodel, and queues"),
        el("div", { class: "inspection-grid" }, this.definitionView, detail));
      this.main.replaceChildren(el("div", { class: "detail-shell" }, el("div", { class: "detail-toolbar" }, back), summary,
        el("div", { class: "operations-grid" }, timeline, this.eventForm), progressive));
    }
  }

  disconnectedCallback() { this.source?.close(); clearTimeout(this.refreshTimer); clearTimeout(this.directoryTimer); clearInterval(this.relativeTimer); }
}
customElements.define("inspector-app", InspectorApp);
