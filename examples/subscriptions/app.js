const SCENARIOS = {
  success: 'The processor settles the charge immediately.',
  hard_decline: 'The processor declines permanently; entitlement is disabled.',
  retryable_decline: 'The first attempt declines, then a fresh retry succeeds.',
  communication_failure: 'The charge succeeds but its acknowledgement fails; reconciliation looks it up without charging again.',
  accepted_delayed_success: 'The processor accepts the charge, then delivers settlement after a delay.',
  duplicate_result: 'The same settlement callback arrives twice and the duplicate is counted.',
  stale_out_of_order: 'An older settlement arrives after the current result and is rejected as stale.',
  lost_result: 'The settlement callback is lost; lookup recovers the durable processor result.',
  idempotency_replay: 'The same idempotency key is replayed and resolves to the original payment.'
};
const el=(tag,attrs={},text='')=>{const n=document.createElement(tag);for(const [k,v] of Object.entries(attrs)){if(k==='class')n.className=v;else if(k==='dataset')Object.assign(n.dataset,v);else n.setAttribute(k,v)}if(text!=='' )n.textContent=String(text);return n};
const money=(minor,currency='USD')=>new Intl.NumberFormat(undefined,{style:'currency',currency}).format((Number(minor)||0)/100);
const value=(x,fallback='—')=>x===undefined||x===null||x===''?fallback:String(x);
const billingState=x=>(x.states||[]).find(s=>['setup','charging','awaiting','retry_wait','reconciliation','current','past_due','cancelled'].includes(s))||'unknown';
const entitlementState=x=>(x.states||[]).find(s=>['grace','enabled','disabled','cancelled'].includes(s))||'unknown';
function fact(label,val,mono=false){const d=el('div'),dt=el('dt',{},label),dd=el('dd');if(mono)dd.append(el('code',{},value(val)));else dd.textContent=value(val);d.append(dt,dd);return d}
function copyButton(label,id){const b=el('button',{class:'copy',type:'button','aria-label':`Copy ${label}`},'Copy');b.addEventListener('click',async()=>{try{await navigator.clipboard.writeText(String(id));b.textContent='Copied'}catch{b.textContent='Copy failed'}});return b}
function identityFact(label,id){const d=el('div',{class:'identity-fact'}),dt=el('dt',{},label),dd=el('dd');dd.append(el('code',{},value(id)));if(id)dd.append(copyButton(label,id));d.append(dt,dd);return d}
function scenarioSelect(name){const s=el('select',{name,class:'scenario-select','aria-label':'Processor scenario'});for(const key of Object.keys(SCENARIOS))s.append(el('option',{value:key},key.replaceAll('_',' ')));return s}

class SubscriptionDirectory extends HTMLElement{
  connectedCallback(){
    this.render([],null,[])
  }
  render(ids,selected,plans){
    const signature=JSON.stringify([ids,selected,plans]);
    if(signature===this.signature)return;
    this.signature=signature;
    this.replaceChildren();
    const head=el('div',{class:'panel-head'});
    head.append(el('h1',{},'Subscriptions'),el('span',{class:'count'},`${ids.length} ${ids.length===1?'actor':'actors'}`));
    const list=el('ul',{class:'subscription-list','aria-label':'Subscription directory'});
    let active;
    if(!ids.length)list.append(el('li',{class:'empty'},'No subscriptions yet. Create one below.'));
    for(const id of ids){
      const b=el('button',{type:'button','aria-current':id===selected},id);
      if(id===selected)active=b;
      b.addEventListener('click',()=>this.dispatchEvent(new CustomEvent('select',{detail:id,bubbles:true})));
      const item=el('li');
      item.append(b);
      list.append(item);
    }
    const create=el('details',{class:'create-panel'});
    create.open=matchMedia('(min-width:701px)').matches;
    create.append(el('summary',{},'Create subscription'));
    const form=el('form');
    const idField=this.field('Subscription ID',el('input',{name:'ID',value:'sub.demo',required:'','aria-describedby':'create-help'}));
    const plan=el('select',{name:'Plan','aria-label':'Plan'});
    for(const p of plans)plan.append(el('option',{value:p.name},`${p.name} — ${money(p.unit_amount,p.currency)}`));
    const quantity=el('input',{name:'Quantity',type:'number',value:'1',min:'1',max:'100',required:''});
    const scenario=scenarioSelect('Scenario');
    form.append(idField,this.field('Plan',plan),this.field('Quantity',quantity),this.field('Initial scenario',scenario),el('p',{id:'create-help',class:'price-note'},'Prices come from the server plan catalog. The browser submits plan and quantity only—never a price.'),el('p',{class:'error',role:'alert'}),el('button',{class:'primary',type:'submit'},'Create subscription'));
    form.addEventListener('submit',event=>{
      event.preventDefault();
      const data=new FormData(form);
      this.dispatchEvent(new CustomEvent('create',{detail:{ID:data.get('ID'),Plan:data.get('Plan'),Quantity:Number(data.get('Quantity')),Scenario:data.get('Scenario')},bubbles:true}));
    });
    create.append(form);
    this.append(head,list,create);
    this.form=form;
    if(active&&matchMedia('(max-width:700px)').matches)requestAnimationFrame(()=>list.scrollTo({left:Math.max(0,active.offsetLeft-12)}));
  }
  field(label,node){
    const field=el('div',{class:'field'}),text=el('label',{},label);
    text.append(node);
    field.append(text);
    return field;
  }
  pending(on,error=''){
    if(!this.form)return;
    this.form.querySelector('button').disabled=on;
    this.form.querySelector('.error').textContent=error;
  }
}
customElements.define('subscription-directory',SubscriptionDirectory);

class SubscriptionWorkspace extends HTMLElement{
  render(x,pending=false){this.replaceChildren();if(!x){this.append(el('div',{class:'empty'},'Select or create a subscription to open its authoritative workspace.'));return}const header=el('header',{class:'workspace-header'});header.append(el('h1',{},x.id),el('p',{},`Authoritative actor projection · ${value(x.currency,'USD')}`));const regions=el('div',{class:'state-regions'});regions.append(this.region('Billing',billingState(x),[['Period',x.period],['Paid period',x.paid_period],['Attempt',`${value(x.attempt,'0')} / ${value(x.max_attempts,'0')}`],['Last result',x.last_result]]),this.region('Entitlement',entitlementState(x),[['Plan',x.plan],['Quantity',x.quantity],['Exact price',money(x.amount,x.currency)],['Paid through',Number(x.paid_period)>0?`Period ${x.paid_period}`:'Not yet paid']]));const tech=el('section',{class:'technical'});tech.append(el('h2',{},'Operation identity'));const facts=el('dl',{class:'facts'});facts.append(identityFact('Operation',x.operation),identityFact('Correlation',x.correlation),identityFact('Idempotency key',x.idempotency_key),identityFact('Last result ID',x.last_result_id),fact('Duplicate results',x.duplicate_count??0),fact('Stale results',x.stale_count??0));tech.append(facts);const scenario=el('section',{class:'scenario-block'}),title=el('h2',{},'Processor scenario'),control=el('div',{class:'scenario-control'}),select=scenarioSelect('scenario');select.value=x.scenario;const description=el('p',{class:'scenario-copy'},SCENARIOS[x.scenario]||'Scenario behavior is controlled by the server.');select.addEventListener('change',()=>{select.disabled=true;this.dispatchEvent(new CustomEvent('command',{detail:{name:'scenario',body:{Scenario:select.value}},bubbles:true}))});control.append(select,description);scenario.append(title,control,el('p',{class:'scenario-authority'},'Changing this sends a command. The display updates only after GET/SSE confirmation.'));const commands=el('section',{class:'commands'});commands.append(el('h2',{},'Actor commands'));const actions=el('div',{class:'command-row'}),cancelled=(x.states||[]).includes('cancelled'),retryUseful=['past_due','retry_wait','reconciliation'].includes(billingState(x));for(const [name,label,cls,disabled] of [['retry','Retry charge','secondary',!retryUseful],['advance','Advance billing period','',billingState(x)!=='current'],['cancel','Cancel subscription','danger',cancelled]]){const b=el('button',{type:'button',class:`command ${cls}`,'data-command':name},label);b.disabled=pending||disabled;b.addEventListener('click',()=>this.dispatchEvent(new CustomEvent('command',{detail:{name},bubbles:true})));actions.append(b)}commands.append(actions);this.append(header,regions,tech,scenario,commands)}
  region(label,state,items){const s=el('section',{class:'state-region'});s.append(el('div',{class:'region-label'},label),el('div',{class:'state-name'},state.replaceAll('_',' ')));const dl=el('dl',{class:'facts'});for(const item of items)dl.append(fact(item[0],item[1]));s.append(dl);return s}
}
customElements.define('subscription-workspace',SubscriptionWorkspace);

class ProcessorActivity extends HTMLElement{
  render(rows=[]){this.replaceChildren();const head=el('div',{class:'panel-head'});head.append(el('h2',{},'Processor activity'),el('span',{class:'count'},'Newest first'));this.append(head);if(!rows.length){this.append(el('p',{class:'empty'},'No processor requests or results have been recorded for this subscription.'));return}const list=el('ol',{class:'activity-list','aria-label':'Newest processor activity'});for(const a of rows){const labels=a.kind==='lookup'?['reconciliation lookup','requested']:a.kind==='communication_error'?['charge transport','delivery failed']:[value(a.scenario,a.kind||'event'),a.status||'recorded'],status=labels[1],item=el('li',{class:'activity-record'}),top=el('div',{class:'activity-summary'});top.append(el('strong',{},labels[0].replaceAll('_',' ')),el('span',{class:`status ${a.kind||status}`},status.replaceAll('_',' ')));const metrics=el('dl',{class:'activity-metrics'});metrics.append(fact('Amount',a.amount===undefined?'—':money(a.amount,a.currency)),fact('Attempt',a.attempt??'—'));const ids=el('dl',{class:'activity-identifiers'});for(const [label,id] of [['Idempotency key',a.key],['Correlation',a.correlation],['Result ID',a.result_id]])if(id)ids.append(identityFact(label,id));item.append(top,metrics);if(ids.children.length)item.append(ids);list.append(item)}this.append(list)}
}
customElements.define('processor-activity',ProcessorActivity);

class SubscriptionConsole extends HTMLElement{
  constructor(){super();this.ids=[];this.selected=null;this.state=null;this.generation=0;this.pending=false;this.plans=[]}
  connectedCallback(){const grid=el('div',{class:'console-grid'});this.directory=el('subscription-directory',{class:'directory'});this.workspace=el('subscription-workspace',{class:'workspace'});this.activity=el('processor-activity',{class:'activity-rail'});grid.append(this.directory,this.workspace,this.activity);this.append(grid);this.addEventListener('select',e=>this.select(e.detail));this.addEventListener('create',e=>this.create(e.detail));this.addEventListener('command',e=>this.command(e.detail));this.boot()}
  async boot(){try{const [ls,plans]=await Promise.all([this.request('/api/subscriptions'),this.request('/api/plans')]);this.ids=ls.subscriptions||[];this.plans=plans.plans||[];this.draw();if(this.ids.length)this.select(this.ids[0])}catch(e){this.connection('offline');this.announce(e.message)}}
  async request(url,options){const r=await fetch(url,options);if(!r.ok)throw new Error((await r.text()).trim()||`Request failed (${r.status})`);if(r.status===204)return null;return r.json()}
  draw(){this.directory.render(this.ids,this.selected,this.plans);this.workspace.render(this.state,this.pending);this.activity.render(this.state?.activity||[])}
  async select(id){if(!id||id===this.selected&&this.stream)return;this.selected=id;this.state=null;const generation=++this.generation;if(this.stream)this.stream.close();this.draw();try{const state=await this.request(`/api/subscriptions/${encodeURIComponent(id)}`);if(generation!==this.generation)return;this.state=state;this.draw();this.connect(id,generation)}catch(e){if(generation===this.generation)this.announce(e.message)}}
  connect(id,generation){const stream=new EventSource(`/api/subscriptions/${encodeURIComponent(id)}/events`);this.stream=stream;this.connection('reconnecting');stream.onopen=()=>{if(generation===this.generation)this.connection('live')};stream.onerror=()=>{if(generation===this.generation)this.connection(navigator.onLine?'reconnecting':'offline')};stream.addEventListener('state',e=>{if(generation!==this.generation||stream!==this.stream)return;try{const state=JSON.parse(e.data);if(state.id!==id)return;this.state=state;this.pending=false;this.draw()}catch{this.announce('A state update could not be read.')}})}
  async create(body){this.directory.pending(true);try{const created=await this.request('/api/subscriptions',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const ls=await this.request('/api/subscriptions');this.ids=ls.subscriptions||[];this.directory.pending(false);await this.select(created.id)}catch(e){this.directory.pending(false,e.message)}}
  async command({name,body}){if(!this.selected||this.pending)return;this.pending=true;this.draw();try{await this.request(`/api/subscriptions/${encodeURIComponent(this.selected)}/${name}`,{method:'POST',headers:body?{'Content-Type':'application/json'}:undefined,body:body?JSON.stringify(body):undefined});this.announce(`${name.replaceAll('_',' ')} command accepted. Waiting for authoritative state.`)}catch(e){this.pending=false;this.draw();this.announce(e.message)}}
  connection(state){const n=document.querySelector('#connection');n.className=`connection ${state}`;n.textContent=state==='live'?'Live':state[0].toUpperCase()+state.slice(1)}
  announce(text){document.querySelector('#announcer').textContent=text}
}
customElements.define('subscription-console',SubscriptionConsole);
