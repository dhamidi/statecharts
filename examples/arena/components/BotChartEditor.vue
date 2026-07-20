<template>
  <div class="workbench">
    <header class="top">
      <div><div class="eyebrow">LIVE DEFINITION / COMPOSABLE GO CAPABILITIES</div><h1>BOT LOGIC WORKBENCH</h1><a href="/">← Arena</a></div>
      <div class="revision"><span class="label">Published revision</span><code id="current">—</code></div>
    </header>
    <nav class="toolbar">
      <button id="validate" class="primary">Validate</button>
      <button id="publish" class="publish">Publish revision</button>
      <button id="reload">Reload published</button>
      <span class="divider"></span><span class="label">Deploy</span><span id="fleet" class="deploy"></span><button id="rollout-all">ALL</button>
      <span id="status" class="status">LOADING</span>
    </nav>
    <section class="metadata">
      <label><span>Chart ID</span><input id="chart-id" readonly></label>
      <label><span>Name</span><input id="chart-name" placeholder="Optional display name"></label>
      <label><span>Revision salt</span><input id="revision-salt" placeholder="Bump when Go behavior changes"></label>
    </section>
    <main class="workspace">
      <aside class="panel">
        <div class="panel-head"><h2>STATES &amp; TRANSITIONS</h2><button data-command="add-child">+ STATE</button></div>
        <div class="panel-body"><div id="state-tree"></div></div>
      </aside>
      <section class="panel">
        <div class="panel-head"><h2 id="state-title">STATE</h2><button id="delete-state" class="danger" data-command="delete-state">DELETE STATE</button></div>
        <div id="inspector" class="panel-body"></div>
      </section>
    </main>
    <section class="panel" style="margin-top:12px">
      <div class="panel-head"><h2>CAPABILITY MAP</h2><span class="label">BUILD DECISIONS FROM CONDITIONS + ACTIONS</span></div>
      <div id="capability-map" class="panel-body capability-map"></div>
    </section>
    <details class="advanced">
      <summary>Advanced · canonical Definition JSON escape hatch</summary>
      <textarea id="raw-definition" spellcheck="false"></textarea>
      <div class="raw-actions"><button id="apply-raw">Apply JSON to form</button><button id="copy-raw">Copy JSON</button></div>
    </details>
  </div>
</template>

<script customelement>
class BotChartEditor extends HTMLElement {
  connectedCallback() {
    if (this.initialized) {
      this.startPolling()
      return
    }
    this.initialized = true
    this.definition = null
    this.vocabulary = { actions: [], conditions: [], events: [], states: [] }
    this.bots = []
    this.selected = 'root'
    this.openTransition = null
    this.busy = true
    this.bind()
    this.initialize()
    this.startPolling()
  }

  disconnectedCallback() {
    clearInterval(this.poll)
    this.poll = null
  }

  startPolling() {
    if (this.poll == null) this.poll = setInterval(() => this.refreshFleet(), 2000)
  }

  bind() {
    this.querySelector('#validate').onclick = () => this.validate()
    this.querySelector('#publish').onclick = () => this.publish()
    this.querySelector('#reload').onclick = () => this.loadDefinition()
    this.querySelector('#rollout-all').onclick = () => this.rollout('/bots/rollout')
    this.querySelector('#apply-raw').onclick = () => this.applyRaw()
    this.querySelector('#copy-raw').onclick = () => navigator.clipboard.writeText(this.querySelector('#raw-definition').value)
    this.addEventListener('click', event => this.handleClick(event))
    this.addEventListener('change', event => this.handleChange(event))
    this.querySelector('#chart-name').oninput = event => { this.definition.name = event.target.value; this.dirty() }
    this.querySelector('#revision-salt').oninput = event => { this.definition.revisionSalt = event.target.value; this.dirty() }
  }

  async initialize() {
    try {
      await Promise.all([this.loadVocabulary(), this.loadDefinition(), this.refreshFleet()])
      this.setStatus('PUBLISHED', true)
    } catch (error) {
      this.setStatus(error.message, false, true)
    } finally {
      this.busy = false
      this.render()
      this.setDisabled()
    }
  }

  async request(path, options = {}) {
    const response = await fetch(path, options)
    const text = await response.text()
    if (!response.ok) throw new Error(text.trim() || `${response.status}`)
    return { response, text, data: text ? JSON.parse(text) : null }
  }

  async loadVocabulary() {
    const { data } = await this.request('/definitions/bot/vocabulary')
    this.vocabulary = data
  }

  async loadDefinition() {
    this.busy = true
    this.setDisabled()
    try {
      const { response, data } = await this.request('/definitions/bot')
      this.definition = data
      this.current = response.headers.get('X-Statechart-Revision') || ''
      this.selected = 'root'
      this.setStatus('PUBLISHED', true)
    } finally {
      this.busy = false
      this.render()
      this.setDisabled()
    }
  }

  render() {
    if (!this.definition) return
    this.querySelector('#current').textContent = this.current || '—'
    this.querySelector('#current').title = this.current || ''
    this.querySelector('#chart-id').value = this.definition.id || ''
    this.querySelector('#chart-name').value = this.definition.name || ''
    this.querySelector('#revision-salt').value = this.definition.revisionSalt || ''
    this.renderTree()
    this.renderInspector()
    this.renderCapabilities()
    this.renderFleet()
    this.syncRaw()
  }

  stateAt(path) {
    let state = this.definition.root
    if (path === 'root') return state
    for (const part of path.slice(5).split('.')) state = state.children[Number(part)]
    return state
  }

  parentOf(path) {
    const parts = path.slice(5).split('.')
    const index = Number(parts.pop())
    const parentPath = parts.length ? `root.${parts.join('.')}` : 'root'
    return { parent: this.stateAt(parentPath), parentPath, index }
  }

  renderTree() {
    const branch = (state, path) => `<li><div class="state-node"><button class="${path === this.selected ? 'selected' : ''}" data-command="select-state" data-path="${path}">${this.escape(state.id?.value || '(generated)')}<span class="state-kind">${this.escape(state.kind)}</span></button></div>${(state.children || []).length ? `<ul class="state-tree">${state.children.map((child, index) => branch(child, `${path}.${index}`)).join('')}</ul>` : ''}</li>`
    this.querySelector('#state-tree').innerHTML = `<ul class="state-tree">${branch(this.definition.root, 'root')}</ul>`
  }

  renderInspector() {
    const state = this.stateAt(this.selected)
    this.querySelector('#state-title').textContent = `STATE · ${state.id?.value || '(generated)'}`
    this.querySelector('#delete-state').hidden = this.selected === 'root'
    const initialTarget = state.initial?.targets?.join(', ') || ''
    const inspector = this.querySelector('#inspector')
    inspector.innerHTML = `
      <div class="state-fields">
        <label><span>State ID</span><input data-state-field="id" value="${this.escape(state.id?.value || '')}"></label>
        <label><span>Kind</span><select data-state-field="kind">${this.vocabulary.states.map(kind => `<option ${kind === state.kind ? 'selected' : ''}>${this.escape(kind)}</option>`).join('')}</select></label>
        <label><span>Initial target</span><input data-state-field="initial" value="${this.escape(initialTarget)}" placeholder="For compound states"></label>
      </div>
      <div class="section-title"><h3>Transitions · priority order</h3><button data-command="add-transition">+ TRANSITION</button></div>
      <div id="transitions">${(state.transitions || []).map((transition, index) => this.transitionForm(transition, index)).join('') || '<div class="empty">No transitions. Add one to define how this state reacts.</div>'}</div>`
    inspector.querySelectorAll('details.transition').forEach(details => details.ontoggle = () => {
      if (details.open) this.openTransition = Number(details.dataset.transition)
      else if (this.openTransition === Number(details.dataset.transition)) this.openTransition = null
    })
  }

  transitionForm(transition, index) {
    const conditionName = this.referenceName(transition.condition)
    const descriptor = this.vocabulary.conditions.find(item => item.name === conditionName)
    const events = (transition.events || []).join(', ') || 'eventless'
    const guard = descriptor ? descriptor.name.replace('arena.bot.', '') : 'always'
    const actions = (transition.actions || []).flat().map(action => action.call?.function?.name?.replace('arena.bot.', '') || action.kind).join(' + ') || 'state change only'
    return `<details class="transition" data-transition="${index}" ${this.openTransition === index ? 'open' : ''}>
      <summary class="transition-head"><strong>${index + 1}<span>${this.escape(events)} · ${this.escape(guard)} → ${this.escape(actions)}</span></strong><button data-command="move-transition-up" data-index="${index}">↑</button><button data-command="move-transition-down" data-index="${index}">↓</button><button class="danger" data-command="remove-transition" data-index="${index}">REMOVE</button></summary>
      <div class="transition-grid">
        <label><span>On event(s)</span><input data-transition-field="events" data-index="${index}" value="${this.escape((transition.events || []).join(', '))}" placeholder="match.snapshot"></label>
        <label><span>Target state(s)</span><input data-transition-field="targets" data-index="${index}" value="${this.escape((transition.targets || []).join(', '))}" placeholder="Targetless = stay here"></label>
        <label><span>Type</span><select data-transition-field="type" data-index="${index}"><option value="external" ${transition.type !== 'internal' ? 'selected' : ''}>external</option><option value="internal" ${transition.type === 'internal' ? 'selected' : ''}>internal</option></select></label>
      </div>
      <div class="logic-row"><span class="label">IF · optional guard</span>${this.conditionForm(index, transition, descriptor)}</div>
      <div class="logic-row"><span class="label">THEN · ordered capabilities</span>${this.actionForms(index, transition)}<button data-command="add-action" data-index="${index}">+ ACTION</button></div>
    </details>`
  }

  conditionForm(transitionIndex, transition, descriptor) {
    const name = descriptor?.name || ''
    return `<div class="capability"><label><span>Condition</span><select data-condition data-index="${transitionIndex}"><option value="">always</option>${this.vocabulary.conditions.map(item => `<option value="${this.escape(item.name)}" ${item.name === name ? 'selected' : ''} ${this.supportsEvents(item, transition) ? '' : 'disabled'}>${this.escape(item.category)} · ${this.escape(item.name.replace('arena.bot.', ''))}</option>`).join('')}</select></label>${descriptor ? this.parameterFields(descriptor, transition.condition, 'condition', { transitionIndex }) : ''}${descriptor ? `<p>${this.escape(descriptor.summary)}</p>` : ''}</div>`
  }

  actionForms(transitionIndex, transition) {
    const rows = []
    ;(transition.actions || []).forEach((block, blockIndex) => block.forEach((executable, actionIndex) => {
      const name = executable.call?.function?.name || ''
      const descriptor = this.vocabulary.actions.find(item => item.name === name)
      rows.push(`<div class="action-row"><div class="capability"><label><span>Action</span><select data-action data-transition-index="${transitionIndex}" data-block-index="${blockIndex}" data-action-index="${actionIndex}">${this.vocabulary.actions.map(item => `<option value="${this.escape(item.name)}" ${item.name === name ? 'selected' : ''} ${this.supportsEvents(item, transition) ? '' : 'disabled'}>${this.escape(item.category)} · ${this.escape(item.name.replace('arena.bot.', ''))}</option>`).join('')}</select></label>${descriptor ? this.parameterFields(descriptor, executable, 'action', { transitionIndex, blockIndex, actionIndex }) : ''}<button class="danger" data-command="remove-action" data-index="${transitionIndex}" data-block-index="${blockIndex}" data-action-index="${actionIndex}">×</button>${descriptor ? `<p>${this.escape(descriptor.summary)}</p>` : `<p>Unsupported executable kind ${this.escape(executable.kind)}; use Advanced JSON to edit it.</p>`}</div></div>`)
    }))
    return rows.join('') || '<div class="empty">No action: this transition only changes state.</div>'
  }

  parameterFields(descriptor, owner, kind, location) {
    return (descriptor.parameters || []).map((parameter, index) => {
      const value = this.argumentValue(owner, kind, index, parameter.default)
      const attrs = kind === 'condition' ? `data-condition-param data-index="${location.transitionIndex}"` : `data-action-param data-transition-index="${location.transitionIndex}" data-block-index="${location.blockIndex}" data-action-index="${location.actionIndex}"`
      if (parameter.type === 'enum') {
        return `<label><span>${this.escape(parameter.label)}</span><select ${attrs} data-param-index="${index}">${parameter.options.map(option => `<option ${option === value ? 'selected' : ''}>${this.escape(option)}</option>`).join('')}</select></label>`
      }
      return `<label><span>${this.escape(parameter.label)}</span><input type="number" step="1" ${attrs} data-param-index="${index}" min="${parameter.minimum ?? ''}" value="${this.escape(value)}"></label>`
    }).join('')
  }

  renderCapabilities() {
    const cards = (kind, items) => items.map(item => `<article class="capability-card"><span class="label">${this.escape(kind)} · ${this.escape(item.category)}</span><br><code>${this.escape(item.name.replace('arena.bot.', ''))}</code>${(item.parameters || []).length ? `<span class="label">(${item.parameters.map(parameter => this.escape(parameter.name)).join(', ')})</span>` : ''}<p>${this.escape(item.summary)}</p></article>`).join('')
    this.querySelector('#capability-map').innerHTML = cards('action', this.vocabulary.actions) + cards('condition', this.vocabulary.conditions)
  }

  handleClick(event) {
    const button = event.target.closest('[data-command]')
    if (!button || this.busy || !this.definition) return
    event.preventDefault()
    const state = this.stateAt(this.selected)
    const index = Number(button.dataset.index)
    switch (button.dataset.command) {
      case 'select-state': this.selected = button.dataset.path; this.renderTree(); this.renderInspector(); this.syncRaw(); return
      case 'add-child': {
        state.children ||= []
        const id = this.uniqueStateID()
        state.children.push({ id: { value: id }, kind: 'atomic' })
        this.selected = `${this.selected}.${state.children.length - 1}`
        break
      }
      case 'delete-state': {
        const { parent, parentPath, index: childIndex } = this.parentOf(this.selected)
        parent.children.splice(childIndex, 1)
        this.selected = parentPath
        break
      }
      case 'add-transition': state.transitions = [...(state.transitions || []), { events: ['match.snapshot'], type: 'external', actions: [] }]; this.openTransition = state.transitions.length - 1; break
      case 'remove-transition': state.transitions.splice(index, 1); break
      case 'move-transition-up': if (index > 0) [state.transitions[index - 1], state.transitions[index]] = [state.transitions[index], state.transitions[index - 1]]; break
      case 'move-transition-down': if (index < state.transitions.length - 1) [state.transitions[index + 1], state.transitions[index]] = [state.transitions[index], state.transitions[index + 1]]; break
      case 'add-action': {
        const transition = state.transitions[index]
        const descriptor = this.vocabulary.actions.find(item => this.supportsEvents(item, transition))
        if (!descriptor) {
          this.setStatus('No registered action supports this transition event', false, true)
          return
        }
        state.transitions[index].actions ||= []
        state.transitions[index].actions.push([this.clone(descriptor.example)])
        break
      }
      case 'remove-action': {
        const blocks = state.transitions[index].actions
        const blockIndex = Number(button.dataset.blockIndex)
        blocks[blockIndex].splice(Number(button.dataset.actionIndex), 1)
        if (!blocks[blockIndex].length) blocks.splice(blockIndex, 1)
        break
      }
      default: return
    }
    this.dirty()
    this.renderTree()
    this.renderInspector()
  }

  handleChange(event) {
    if (!this.definition) return
    const state = this.stateAt(this.selected)
    const target = event.target
    if (target.dataset.stateField) {
      if (target.dataset.stateField === 'id') state.id = { value: target.value }
      if (target.dataset.stateField === 'kind') state.kind = target.value
      if (target.dataset.stateField === 'initial') state.initial = target.value.trim() ? { targets: this.csv(target.value), type: 'external' } : undefined
      this.dirty(); this.renderTree(); return
    }
    const transitionIndex = Number(target.dataset.index ?? target.dataset.transitionIndex)
    const transition = state.transitions?.[transitionIndex]
    if (!transition) return
    let rerender = false
    if (target.dataset.transitionField) {
      transition[target.dataset.transitionField] = target.dataset.transitionField === 'type' ? target.value : this.csv(target.value)
      rerender = target.dataset.transitionField === 'events'
    } else if (target.hasAttribute('data-condition')) {
      const descriptor = this.vocabulary.conditions.find(item => item.name === target.value)
      transition.condition = descriptor ? this.clone(descriptor.example) : undefined
      this.renderInspector()
    } else if (target.hasAttribute('data-condition-param')) {
      const descriptor = this.vocabulary.conditions.find(item => item.name === this.referenceName(transition.condition))
      this.setArgument(transition.condition, 'condition', Number(target.dataset.paramIndex), descriptor.parameters[Number(target.dataset.paramIndex)], target.value)
    } else if (target.hasAttribute('data-action')) {
      const executable = transition.actions[Number(target.dataset.blockIndex)][Number(target.dataset.actionIndex)]
      const descriptor = this.vocabulary.actions.find(item => item.name === target.value)
      Object.keys(executable).forEach(key => delete executable[key])
      Object.assign(executable, this.clone(descriptor.example))
      this.renderInspector()
    } else if (target.hasAttribute('data-action-param')) {
      const executable = transition.actions[Number(target.dataset.blockIndex)][Number(target.dataset.actionIndex)]
      const descriptor = this.vocabulary.actions.find(item => item.name === executable.call?.function?.name)
      this.setArgument(executable, 'action', Number(target.dataset.paramIndex), descriptor.parameters[Number(target.dataset.paramIndex)], target.value)
    } else return
    this.dirty()
    if (rerender) this.renderInspector()
  }

  argumentValue(owner, kind, index, fallback) {
    const expression = kind === 'action' ? owner.call?.function?.args?.[index] : this.conditionArguments(owner)?.[index]
    if (!expression || expression.kind !== 'go.literal') return fallback
    if (expression.data.kind === 'string') return expression.data.string
    if (expression.data.kind === 'number') return this.integerFromCanonical(expression.data.number)
    return fallback
  }

  setArgument(owner, kind, index, parameter, value) {
    const expression = this.literalExpression(parameter, value)
    if (kind === 'action') {
      owner.call.function.args ||= []
      owner.call.function.args[index] = expression
      return
    }
    const items = owner.data.map.args.list
    const encoded = { version: 1, kind: 'map', map: { kind: { version: 1, kind: 'string', string: expression.kind }, data: expression.data } }
    items[index] = encoded
  }

  conditionArguments(expression) {
    return expression?.data?.map?.args?.list?.map(item => ({ kind: item.map.kind.string, data: item.map.data })) || []
  }

  literalExpression(parameter, value) {
    if (parameter.type === 'integer') return { kind: 'go.literal', data: { version: 1, kind: 'number', number: this.canonicalInteger(value) } }
    return { kind: 'go.literal', data: { version: 1, kind: 'string', string: value } }
  }

  canonicalInteger(value) {
    const input = String(value || 0)
    if (!/^-?\d+$/.test(input)) return input
    let text = String(BigInt(input))
    if (text === '0') return '0'
    const sign = text.startsWith('-') ? '-' : ''
    if (sign) text = text.slice(1)
    let exponent = 0
    while (text.endsWith('0')) { text = text.slice(0, -1); exponent++ }
    return sign + text + (exponent ? `e${exponent}` : '')
  }

  integerFromCanonical(value) {
    const match = String(value).match(/^(-?\d+)(?:e(\d+))?$/)
    if (!match) return value
    return String(BigInt(match[1]) * (10n ** BigInt(match[2] || 0)))
  }

  referenceName(expression) { return expression?.data?.map?.name?.string || '' }
  supportsEvents(capability, transition) {
    const events = transition.events || []
    return events.length > 0 && events.every(event => (capability.events || []).includes(event))
  }
  csv(value) { return value.split(',').map(item => item.trim()).filter(Boolean) }
  clone(value) { return JSON.parse(JSON.stringify(value)) }
  escape(value) { return String(value ?? '').replace(/[&<>"']/g, character => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[character]) }

  uniqueStateID() {
    const ids = new Set()
    const visit = state => { ids.add(state.id?.value); (state.children || []).forEach(visit) }
    visit(this.definition.root)
    let number = ids.size
    while (ids.has(`state-${number}`)) number++
    return `state-${number}`
  }

  dirty() { this.setStatus('DIRTY'); this.syncRaw() }
  syncRaw() {
    const raw = this.querySelector('#raw-definition')
    if (document.activeElement !== raw) raw.value = JSON.stringify(this.definition, null, 2) + '\n'
  }

  applyRaw() {
    try {
      this.definition = JSON.parse(this.querySelector('#raw-definition').value)
      this.selected = 'root'
      this.dirty()
      this.render()
    } catch (error) {
      this.setStatus(`JSON: ${error.message}`, false, true)
    }
  }

  async validate() { return this.submitDefinition('/definitions/bot/validate', 'POST', 'VALID') }
  async publish() {
    const result = await this.submitDefinition('/definitions/bot', 'PUT', 'PUBLISHED')
    if (result) { this.current = result.revision; this.render(); await this.refreshFleet() }
  }

  async submitDefinition(path, method, success) {
    this.busy = true
    this.setDisabled()
    try {
      const { data } = await this.request(path, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(this.definition) })
      this.setStatus(`${success} · ${this.shortRevision(data.revision)}`, true)
      return data
    } catch (error) {
      this.setStatus(error.message, false, true)
      return null
    } finally {
      this.busy = false
      this.setDisabled()
    }
  }

  async refreshFleet() {
    try {
      const { data } = await this.request('/bots')
      this.bots = data.bots
      this.renderFleet()
    } catch (error) { this.setStatus(error.message, false, true) }
  }

  renderFleet() {
    const fleet = this.querySelector('#fleet')
    if (!fleet) return
    fleet.innerHTML = this.bots.map(bot => `<button class="${bot.revision === this.current ? 'current' : 'old'}" data-player="${this.escape(bot.player)}" title="${this.escape(bot.controller)} · generation ${bot.generation} · ${this.escape(bot.revision)}">${this.escape(bot.player)} · ${bot.generation}</button>`).join('') || '<span class="label">NO BOTS</span>'
    fleet.querySelectorAll('[data-player]').forEach(button => button.onclick = () => this.rollout(`/bots/${encodeURIComponent(button.dataset.player)}/rollout`))
    this.setDisabled()
  }

  async rollout(path) {
    this.busy = true
    this.setDisabled()
    try {
      await this.request(path, { method: 'POST' })
      await this.refreshFleet()
      this.setStatus('ROLLOUT COMPLETE', true)
    } catch (error) { this.setStatus(error.message, false, true) }
    finally { this.busy = false; this.setDisabled() }
  }

  setDisabled() { this.querySelectorAll('button, input, select, textarea').forEach(control => control.disabled = this.busy) }
  setStatus(message, ok = false, error = false) {
    const status = this.querySelector('#status')
    status.textContent = message
    status.className = `status${ok ? ' ok' : ''}${error ? ' error' : ''}`
  }
  shortRevision(revision) { return revision ? revision.replace('sha256:', '').slice(0, 12) : '—' }
}

customElements.define('bot-chart-editor', BotChartEditor)
</script>
