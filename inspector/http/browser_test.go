package inspectorhttp

import (
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	statecharts "github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/inspector"
)

func TestEmbeddedUIBrowserInteractions(t *testing.T) {
	if os.Getenv("STATECHARTS_BROWSER_TEST") != "1" {
		t.Skip("set STATECHARTS_BROWSER_TEST=1 to run the agent-browser interaction suite")
	}
	if _, err := exec.LookPath("agent-browser"); err != nil {
		t.Skip("agent-browser is not installed")
	}

	received := make(chan statecharts.Event, 4)
	handler, _, _ := testHandler(t, received,
		inspector.WithAuthorizer(inspector.AllowAll()),
		inspector.WithRingSize(8),
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	session := "statecharts-inspector-test-" + strconv.Itoa(os.Getpid())
	browser := func(arguments ...string) string {
		t.Helper()
		args := append([]string{"--session", session}, arguments...)
		command := exec.CommandContext(t.Context(), "agent-browser", args...)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("agent-browser %s: %v\n%s", strings.Join(arguments, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}
	t.Cleanup(func() {
		command := exec.Command("agent-browser", "--session", session, "close")
		_ = command.Run()
	})
	waitFor := func(expression string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			if browser("eval", expression) == "true" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("browser condition did not become true: %s", expression)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	browser("open", server.URL+"/")
	waitFor(`document.querySelector('button[data-actor-id="actor"]') !== null`)
	if got := browser("eval", `document.querySelector('.filter-disclosure')?.open === false && document.querySelector('.directory button')?.getBoundingClientRect().top < innerHeight`); got != "true" {
		t.Fatalf("collapsed filters do not prioritize the actor directory: %s", got)
	}
	browser("click", ".filter-disclosure > summary")
	browser("select", `select[aria-label="residency"]`, "resident")
	waitFor(`document.querySelectorAll('.directory button[data-actor-id="actor"]').length === 1`)
	browser("fill", `input[aria-label="kind"]`, "http-test")
	browser("click", `actor-directory button[type="submit"]`)
	waitFor(`document.querySelectorAll('.directory button[data-actor-id="actor"]').length === 1`)
	browser("click", `button[data-actor-id="actor"]`)
	waitFor(`document.querySelector('.facts .identifier')?.textContent === 'actor'`)
	waitFor(`document.querySelector('.durable-history .empty-state')?.textContent === 'No persisted history'`)
	waitFor(`document.querySelector('.transition[data-transition-state="active"][data-transition-index="0"]') !== null`)
	if got := browser("eval", `(() => {
		const view = document.querySelector('definition-view');
		const transition = view?.querySelector('.transition[data-transition-state="active"][data-transition-index="0"]');
		const text = transition?.textContent || '';
		return view?.querySelectorAll('.definition-tree').length === 1 &&
			view?.textContent.includes('Current is the same as pinned') &&
			text.includes('targetless') && text.includes('external') &&
			text.includes('call record@v1') && !text.includes('(internal)');
	})()`); got != "true" {
		t.Fatalf("definition does not expose targetless transition behavior: %s", got)
	}
	browser("click", `details.disclosure > summary`)
	browser("fill", `definition-view input[type="search"]`, "record@v1")
	if got := browser("eval", `document.querySelector('.transition[data-transition-state="active"][data-transition-index="0"]')?.hidden === false && document.querySelector('.state-def[data-state-id="active"]')?.hidden === false && document.querySelector('definition-view .search-status')?.textContent === '1 matching transition'`); got != "true" {
		t.Fatalf("definition action search did not retain the matching transition: %s", got)
	}
	browser("click", `definition-view .definition-source > summary`)
	if got := browser("eval", `document.querySelector('definition-view .definition-source pre')?.textContent.includes('"http-test.record"') === true`); got != "true" {
		t.Fatalf("definition source does not contain the complete action: %s", got)
	}
	browser("eval", `(() => {
		const expression = {kind:'test.expression',data:{version:1,kind:'string',string:'source'}};
		const call = name => ({kind:'call',call:{function:{name,version:'v3'}}});
		const actions = [[
			{kind:'raise',raise:{event:'raised',data:expression}},
			{kind:'send',send:{event:'sent',target:'target.actor',type:'processor',id:'request',delay:'1s',content:expression}},
			{kind:'cancel',cancel:{sendID:'request'}},
			{kind:'log',log:{label:'audit',expr:expression}},
			{kind:'assign',assign:{location:expression,expr:expression}},
			{kind:'choose',choose:{branches:[{condition:expression,actions:[[call('fixture.branch')]]}],else:[[call('fixture.else')]]}},
			{kind:'foreach',foreach:{array:expression,item:'item',index:'index',actions:[[call('fixture.each')]]}},
			{kind:'script',script:{expr:expression}},
			call('fixture.host'),
			{kind:'extension',extension:{namespace:'urn:fixture',name:'custom',data:{version:1,kind:'null'}}}
		]];
		const state = {id:{value:'fixture'},kind:0,initial:{targets:['fixture'],type:'external'},onEntry:[[call('fixture.enter')]],
			onExit:[[call('fixture.exit')]],transitions:[{events:['everything'],targets:[],type:'internal',condition:expression,actions}],
			data:[{id:'local',expr:expression}],invokes:[{id:'worker',type:'task',src:'job',params:[{name:'input',expr:expression}],
				finalize:[[call('fixture.finalize')]]}],doneData:{content:expression},
			children:[{id:{value:'fixture.shallow'},kind:4,history:0},{id:{value:'fixture.deep'},kind:4,history:1}]};
		const view = document.createElement('definition-view');
		view.id = 'definition-fixture';
		view.data = {Pinned:{ID:'fixture',Datamodel:'test',Root:state},PinnedRevision:'fixture-v1',CurrentAvailable:false};
		document.body.append(view);
		return true;
	})()`)
	if got := browser("eval", `(() => { const text = document.querySelector('#definition-fixture')?.textContent || ''; return [
		'raise raised','send sent → target.actor','cancel request','log audit','assign','choose · 1 branch','foreach item, index',
		'script','call host@v3','extension urn:fixture:custom','Entry actions','Exit actions','Invocations · 1','Done data',
		'History: shallow','History: deep'
	].every(value => text.includes(value)); })()`); got != "true" {
		t.Fatalf("recursive definition renderer omitted executable or state behavior: %s", got)
	}
	browser("click", `#definition-fixture .action.choose > summary`)
	if got := browser("eval", `document.querySelector('#definition-fixture .action.choose .action-branch')?.textContent.includes('call branch@v3') === true`); got != "true" {
		t.Fatalf("compound executable content is not inspectable: %s", got)
	}
	if got := browser("eval", `(() => { const view=document.querySelector('#definition-fixture'); const pinned=view._data.Pinned; view.data={Pinned:pinned,PinnedRevision:'fixture-v1',Current:pinned,CurrentRevision:'fixture-v2',CurrentAvailable:true}; return view.querySelectorAll('.definition-tree').length === 2; })()`); got != "true" {
		t.Fatalf("differing pinned and current revisions are not shown separately: %s", got)
	}
	browser("eval", `document.querySelector('#definition-fixture').remove(); true`)
	if got := browser("eval", `document.querySelector('fieldset.event-data legend')?.textContent === 'Event data' && document.querySelector('select[aria-label="Payload type"]') !== null`); got != "true" {
		t.Fatalf("event payload editor is not explicitly labelled: %s", got)
	}

	browser("fill", `event-form input[aria-label="Event name"]`, "message")
	browser("click", `event-form button[type="submit"]`)
	waitFor(`document.querySelector('event-form [aria-live]')?.textContent === 'Accepted. Not retried.'`)
	waitFor(`document.querySelectorAll('.live-history .live').length > 0`)
	waitFor(`document.querySelectorAll('.transition.selected').length === 1`)
	select {
	case event := <-received:
		if event.Name != "message" || event.Type != statecharts.EventExternal {
			t.Fatalf("browser command delivered %#v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("browser command did not reach the chart")
	}

	browser("eval", `document.querySelector('inspector-app').onObservation({Sequence:998,Timestamp:new Date(Date.now()-1500).toISOString(),Observation:{Timestamp:new Date(Date.now()-1500).toISOString(),Kind:'macrostep',Actor:{ID:'actor'},Macrostep:{Sequence:42,Timestamp:new Date(Date.now()-1500).toISOString(),Trigger:{Name:'message',Type:0},Before:['idle'],After:['active'],Microsteps:[{Trigger:{Name:'message',Type:0},Transitions:[{Source:'active',Index:0}],Exited:['idle'],Entered:['active']}]}}}); true`)
	waitFor(`(() => { const text = document.querySelector('.live-history .live')?.textContent || ''; return /^\d+(?:\.\d)?s ago/.test(text) && text.includes('message [external]') && text.includes('idle → active') && text.includes('transition active[0]') && !text.includes('macrostep'); })()`)
	browser("click", `.live-history button[data-transition-state="active"][data-transition-index="0"]`)
	waitFor(`document.querySelector('details.disclosure')?.open === true && document.querySelector('.transition[data-transition-state="active"][data-transition-index="0"]')?.open === true`)
	if got := browser("eval", `(() => { const view = document.querySelector('inspector-app').definitionView; const transition = document.querySelector('.transition[data-transition-state="active"][data-transition-index="0"]'); return view.focusTransition('active', 0) && document.activeElement === transition.querySelector(':scope > summary'); })()`); got != "true" {
		t.Fatalf("activity navigation did not focus the destination transition: %s", got)
	}
	browser("eval", `document.querySelector('inspector-app').onGap({Sequence:999,Kind:'gap',Reason:'browser test gap',Dropped:2}); true`)
	waitFor(`document.querySelector('.live-history .gap')?.textContent.includes('browser test gap') === true`)
	browser("select", `event-form select[aria-label="Payload type"]`, "map")
	browser("click", "event-form .value-editor > button")
	waitFor(`document.querySelector('event-form input[aria-label="Map key"]') !== null`)

	browser("set", "viewport", "390", "844")
	if got := browser("eval", `document.documentElement.scrollWidth === innerWidth`); got != "true" {
		t.Fatalf("mobile inspector has document-level horizontal overflow: %s", got)
	}
	if got := browser("eval", `[...document.querySelectorAll('button,input,select')].filter(e => e.offsetParent && e.closest('main')).every(e => e.getBoundingClientRect().height >= 44)`); got != "true" {
		t.Fatalf("mobile detail contains undersized controls: %s", got)
	}
	browser("click", ".detail-toolbar .back")
	waitFor(`document.querySelector('actor-directory').offsetParent !== null && document.querySelector('main').offsetParent === null`)
	if got := browser("eval", `document.querySelector('input[aria-label="kind"]').value === 'http-test'`); got != "true" {
		t.Fatalf("mobile Back did not preserve filter state: %s", got)
	}
	browser("click", `button[data-actor-id="actor"]`)
	waitFor(`document.querySelector('main').offsetParent !== null`)
	browser("reload")
	waitFor(`document.querySelector('button[data-actor-id="actor"]') !== null`)
	browser("wait", "300")
	select {
	case duplicate := <-received:
		t.Fatalf("browser reconnect repeated command %#v", duplicate)
	default:
	}

	if output := browser("errors"); output != "" {
		t.Fatalf("browser reported errors:\n%s", output)
	}
}
