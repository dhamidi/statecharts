const TIMELINE_LIMIT = 200;
const RECENT_PAGE_LIMIT = 1000;
const SEEN_SEQUENCE_LIMIT = 1000;

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
    child.value = value;
    child.addEventListener("value-change", () => update(child.value));
    return child;
  }

  render() {
    this.replaceChildren();
    const value = this._value;
    const kinds = ["null", "bool", "number", "string", "list", "map", "tagged"];
    const kind = el("select", {
      "aria-label": "Value kind",
      onchange: event => { this.value = canonicalValue(event.target.value); this.changed(); },
    }, ...kinds.map(name => el("option", { value: name, ...(name === value.kind ? { selected: "" } : {}) }, name)));
    const row = el("div", { class: "row" }, kind);

    if (value.kind === "bool") {
      row.append(el("select", { "aria-label": "Boolean value", onchange: event => { value.bool = event.target.value === "true"; this.changed(); } },
        el("option", { value: "false" }, "false"),
        el("option", { value: "true", ...(value.bool ? { selected: "" } : {}) }, "true")));
    }
    if (value.kind === "number" || value.kind === "string") {
      row.append(el("input", { value: value[value.kind] ?? "", "aria-label": `${value.kind} value`, oninput: event => { value[value.kind] = event.target.value; this.changed(); } }));
    }
    if (value.kind === "tagged") {
      row.append(el("input", { value: value.tag || "", placeholder: "tag", "aria-label": "Payload tag", oninput: event => { value.tag = event.target.value; this.changed(); } }));
    }

    const box = el("div", { class: "value-editor" }, row);
    this.append(box);
    if (value.kind === "list") {
      value.list.forEach((item, index) => box.append(el("div", { class: "value-entry" },
        this.child(item, next => { value.list[index] = next; this.changed(); }),
        el("button", { type: "button", onclick: () => { value.list.splice(index, 1); this.render(); this.changed(); } }, "Remove"))));
      box.append(el("button", { type: "button", onclick: () => { value.list.push(canonicalValue()); this.render(); this.changed(); } }, "Add list item"));
    }
    if (value.kind === "map") this.renderMap(box, value.map);
    if (value.kind === "tagged") box.append(this.child(value.payload, next => { value.payload = next; this.changed(); }));
  }

  renderMap(box, map) {
    for (const [key, item] of Object.entries(map)) {
      const message = el("span", { class: "map-error", "aria-live": "polite" });
      const child = this.child(item, next => {
        if (!Object.hasOwn(map, key)) return;
        Object.defineProperty(map, key, { value: next, writable: true, configurable: true, enumerable: true });
        this.changed();
      });
      const keyInput = el("input", { value: key, "aria-label": "Map key" });
      keyInput.addEventListener("change", () => {
        const next = keyInput.value;
        if (next !== key && Object.hasOwn(map, next)) {
          message.textContent = `Key “${next}” already exists.`;
          keyInput.value = key;
          return;
        }
        const current = map[key];
        delete map[key];
        Object.defineProperty(map, next, { value: current, writable: true, configurable: true, enumerable: true });
        message.textContent = "";
        this.render();
        this.changed();
      });
      box.append(el("div", { class: "map-entry" }, keyInput, child,
        el("button", { type: "button", onclick: () => { delete map[key]; this.render(); this.changed(); } }, "Remove"), message));
    }
    box.append(el("button", { type: "button", onclick: () => {
      let key = "key";
      let suffix = 2;
      while (Object.hasOwn(map, key)) key = `key${suffix++}`;
      Object.defineProperty(map, key, { value: canonicalValue(), writable: true, configurable: true, enumerable: true });
      this.render();
      this.changed();
    } }, "Add map entry"));
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
      el("div", { class: "toolbar" },
        el("input", { type: "search", placeholder: "Actor ID prefix", "aria-label": "Search actors", oninput: event => { this.filters.prefix = event.target.value; } }),
        el("button", { onclick: () => { this.cursor = ""; this.load(); } }, "Refresh")),
      el("div", { class: "filters" },
        el("input", { type: "text", placeholder: "Any kind", "aria-label": "kind", oninput: event => { this.filters.kind = event.target.value.trim(); } }),
        this.select("durable", ["", "true", "false"]), this.select("lifecycle", ["", "active", "terminal"]),
        this.select("residency", ["", "resident", "paged out", "hydrating"])),
      this.message = el("p", { class: "muted" }, "Choose a system."),
      this.list = el("ul", { class: "directory" }),
      this.more = el("button", { onclick: () => this.load(true), disabled: "" }, "Next page")));
  }

  select(name, values) {
    return el("select", { "aria-label": name, onchange: event => { this.filters[name] = event.target.value; this.cursor = ""; this.load(); } },
      ...values.map(value => el("option", { value }, value || `Any ${name}`)));
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
        const button = el("button", { "aria-current": String(id === this.selected), onclick: () => {
          this.selected = id;
          this.dispatchEvent(new CustomEvent("actor-select", { detail: id, bubbles: true }));
          this.loadSelection();
        } }, el("strong", { class: "actor-id" }, id),
        el("span", { class: "actor-meta" }, `${get(actor, "kind") || "unknown"} · ${get(actor, "residency") || "unknown"}`));
        button.dataset.actorId = id;
        this.list.append(el("li", {}, button));
      }
      this.cursor = get(page, "next") || get(page, "nextCursor") || "";
      this.more.toggleAttribute("disabled", !this.cursor);
      this.message.textContent = items.length ? `${this.list.children.length} actor(s)` : "No actors match.";
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
  set data(value) { this._data = value; this.render(); }
  set selectedTransitions(value) { this._selectedTransitions = value || new Set(); if (this._data) this.render(); }

  render() {
    this.replaceChildren();
    if (!this._data) return;
    const pinned = get(this._data, "pinned");
    const currentAvailable = Boolean(get(this._data, "currentAvailable"));
    this.append(el("h2", {}, "Definition"), el("dl", { class: "facts revision-facts" },
      ...field("Pinned revision", get(this._data, "pinnedRevision") || "—"),
      ...field("Current revision", currentAvailable ? get(this._data, "currentRevision") || "—" : "unavailable")));
    if (pinned) this.append(this.definitionSection("Pinned", pinned, true));
    if (currentAvailable) this.append(this.definitionSection("Current", get(this._data, "current"), false));
    else this.append(el("section", {}, el("h3", {}, "Current"), el("p", { class: "muted" }, "Current definition is unavailable.")));
  }

  definitionSection(label, definition, pinned) {
    const section = el("section", {}, el("h3", {}, label));
    section.append(this.state(get(definition, "root"), pinned));
    return section;
  }

  state(state, pinned) {
    if (!state) return el("p", { class: "muted" }, "Unavailable");
    const id = stateID(state);
    const active = this.active?.has(id);
    const node = el("div", { class: `state${active ? " active" : ""}` }, el("strong", {}, `${active ? "● active" : "○ inactive"} ${id} [${stateKind(get(state, "kind"))}]`));
    (get(state, "transitions") || []).forEach((transition, index) => {
      const selected = pinned && this._selectedTransitions?.has(`${id}\u0000${index}`);
      node.append(el("span", { class: `transition${selected ? " selected" : ""}` },
        `${selected ? "▶ selected" : "→"} ${(get(transition, "targets") || []).join(", ") || "(internal)"} on ${(get(transition, "events") || []).join(", ") || "always"}`));
    });
    for (const child of get(state, "children") || []) node.append(this.state(child, pinned));
    return node;
  }
}
customElements.define("definition-view", DefinitionView);

class EventForm extends HTMLElement {
  connectedCallback() {
    if (this.form) return;
    this.value = canonicalValue();
    this.render();
  }
  set target(value) { this._target = value; }

  render() {
    this.editor = document.createElement("value-editor");
    this.editor.value = this.value;
    this.submitButton = el("button", { type: "submit" }, "Send once");
    this.replaceChildren(el("h2", {}, "Send external event"), this.form = el("form", { onsubmit: event => this.submit(event) },
      el("label", {}, "Event name ", this.name = el("input", { required: "", pattern: "[^\\s]+", "aria-label": "Event name" })),
      this.editor, this.submitButton, this.message = el("span", { class: "muted", "aria-live": "polite" })));
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
    this.render();
    this.loadSystems();
  }

  render() {
    this.replaceChildren(el("header", {}, el("h1", {}, "Statechart actor inspector"),
      el("label", {}, "System ", this.picker = el("select", { "aria-label": "System", onchange: event => this.selectSystem(event.target.value) })),
      this.connection = el("span", { class: "status", "data-state": "disconnected" }, "stream disconnected")),
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
    clearTimeout(this.refreshTimer);
    clearTimeout(this.directoryTimer);
    this.directory.system = system;
    this.connect();
    this.main.replaceChildren(el("div", { class: "empty" }, "Select an actor to inspect."));
  }

  connect() {
    this.source?.close();
    if (!this.system) return;
    const version = this.systemGeneration;
    this.connection.textContent = "stream connecting";
    this.connection.dataset.state = "disconnected";
    this.source = new EventSource(`v1/stream?${new URLSearchParams({ system: this.system })}`);
    this.source.onopen = () => { if (version === this.systemGeneration) { this.connection.textContent = "stream connected"; this.connection.dataset.state = "connected"; } };
    this.source.onerror = () => { if (version === this.systemGeneration) { this.connection.textContent = "stream disconnected — manual refresh available"; this.connection.dataset.state = "disconnected"; } };
    this.source.addEventListener("gap", event => { if (version === this.systemGeneration) this.onGap(JSON.parse(event.data)); });
    this.source.addEventListener("observation", event => { if (version === this.systemGeneration) this.onObservation(JSON.parse(event.data)); });
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
      this.connection.textContent = "stream gap — state refresh scheduled";
      this.connection.dataset.state = "gap";
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
    this.connection.textContent = "stream gap — state refresh scheduled";
    this.connection.dataset.state = "gap";
    this.pushTimeline({ class: "gap", sequence: streamSequence, text: `Gap · ${reason}${dropped ? ` · ${dropped} dropped` : ""}` });
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
    const timestamp = get(observation, "timestamp") || get(record, "timestamp") || "unknown time";
    const sequence = macrostep ? ` · macrostep ${get(macrostep, "sequence") ?? "—"}` : "";
    this.pushTimeline({ class: "live", sequence: streamSequence, text: `Live · ${kind} · ${timestamp}${sequence}` });
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
          this.pushTimeline({ class: "gap", text: "Gap · retained live observations expired before catch-up" });
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
        this.pushTimeline({ class: "gap", text: `Recent lossy trace unavailable · ${error.message}` });
        this.scheduleRefresh();
      }
    }
  }

  scheduleDirectoryRefresh() {
    if (this.directoryTimer) return;
    this.directoryTimer = setTimeout(() => { this.directoryTimer = null; this.directory.load(); }, 300);
  }

  scheduleRefresh() {
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
    if (showLoading) this.main.replaceChildren(el("p", {}, "Loading actor…"));
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

  draw(actor, definition, history) {
    const info = get(actor, "info");
    const live = get(actor, "live");
    const facts = el("dl", { class: "facts" }, ...field("ID", get(info, "id")), ...field("Address", get(info, "address")),
      ...field("Kind", get(info, "kind")), ...field("Revision", get(info, "revision")), ...field("Lifecycle", get(info, "lifecycle")),
      ...field("Residency", get(info, "residency")), ...field("Durable", String(get(info, "durable"))));
    const summary = el("section", {}, el("div", { class: "toolbar" }, el("h2", {}, "Actor summary"),
      el("button", { onclick: () => { this.directory.load(); this.refresh(); } }, "Refresh")), facts,
      el("p", { class: "status", "data-state": get(info, "residency") }, live ? "● Live state available" : `○ ${get(info, "residency") || "not resident"} — live state unavailable`));
    const detail = el("section", {}, el("h2", {}, "Live actor detail"));
    if (live) {
      const configuration = get(live, "configuration") || [];
      detail.append(el("p", {}, `Active configuration: ${configuration.join(", ") || "empty"}`));
      detail.append(...this.valuePanel("Canonical datamodel", get(live, "datamodel")));
      detail.append(...this.eventList("Internal queue", get(live, "internalQueue")));
      detail.append(...this.eventList("External queue", get(live, "externalQueue")));
      for (const [key, label] of [["pendingSends", "Pending sends"], ["activeInvokes", "Active invokes"]]) {
        detail.append(el("h3", {}, label), el("pre", { class: "code dense-panel" }, JSON.stringify(get(live, key), null, 2) ?? "—"));
      }
    } else detail.append(el("p", { class: "muted" }, "Actor is not resident; inspection did not hydrate it."));

    const definitionView = document.createElement("definition-view");
    definitionView.active = new Set(live ? get(live, "configuration") || [] : []);
    definitionView.selectedTransitions = this.transitionSet();
    definitionView.data = definition;
    const entries = get(history, "entries") || [];
    const durable = entries.map(entry => {
      const event = get(entry, "event") || {};
      const data = document.createElement("canonical-value");
      data.value = get(event, "data");
      return el("li", { class: "durable" },
        el("strong", {}, `Persisted · seq ${get(entry, "seq")} · ${get(entry, "kind")} · ${get(entry, "timestamp")}`),
        el("div", {}, `Event: ${get(event, "name") || "—"}`), data);
    });
    const timeline = el("section", {}, el("h2", {}, "Timeline"),
      el("p", { class: "muted" }, `Durable persisted history is separate from the newest ${TIMELINE_LIMIT} lossy live observations and gap markers.`),
      el("h3", {}, "Durable history"), el("ul", { class: "timeline durable-history" }, ...durable),
      el("h3", {}, "Lossy live trace"), el("ul", { class: "timeline live-history" }, ...this.timeline.map(item => el("li", { class: item.class }, item.text))));
    if (!this.eventForm) this.eventForm = document.createElement("event-form");
    this.eventForm.target = { system: this.system, id: this.actor };
    this.main.replaceChildren(summary, el("div", { class: "grid" }, detail, definitionView), timeline, this.eventForm);
  }

  disconnectedCallback() { this.source?.close(); clearTimeout(this.refreshTimer); clearTimeout(this.directoryTimer); }
}
customElements.define("inspector-app", InspectorApp);
