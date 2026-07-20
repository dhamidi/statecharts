<template>
  <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <title>Bot Logic Workbench</title>
      <style>
        :root { color-scheme: dark; font: 14px/1.45 ui-monospace, SFMono-Regular, Consolas, monospace; background: #080b10; color: #e8edf3; --panel: #0d131c; --line: #344050; --muted: #8693a5; --cyan: #38d9e6; --green: #4ade80; --yellow: #facc15; --red: #fb7185; }
        * { box-sizing: border-box; }
        body { margin: 0; overflow-x: hidden; }
        button, input, select, textarea { border-radius: 0; font: inherit; }
        button { border: 1px solid var(--line); background: #151e2a; color: #eef4f8; cursor: pointer; padding: 7px 10px; }
        button:hover { background: var(--cyan); border-color: var(--cyan); color: #061014; }
        button:disabled { cursor: wait; opacity: .45; }
        input, select, textarea { background: #070a0f; border: 1px solid var(--line); color: #e8edf3; min-width: 0; padding: 7px 8px; width: 100%; }
        input:focus, select:focus, textarea:focus { border-color: var(--cyan); outline: 1px solid var(--cyan); }
        a { color: var(--cyan); }
        bot-chart-editor { display: block; min-height: 100vh; padding: 18px; }
        .top { align-items: end; border-bottom: 3px solid #edf3f8; display: flex; gap: 24px; justify-content: space-between; padding-bottom: 13px; }
        .eyebrow, .label { color: var(--muted); font-size: 10px; letter-spacing: .13em; text-transform: uppercase; }
        h1 { font: 900 clamp(25px, 4vw, 46px)/.95 ui-monospace, monospace; margin: 4px 0 7px; }
        h2, h3 { font-size: 11px; letter-spacing: .12em; margin: 0; text-transform: uppercase; }
        .revision { max-width: min(52vw, 700px); min-width: 0; text-align: right; }
        .revision code { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .toolbar { align-items: center; border: 1px solid var(--line); border-top: 0; display: flex; flex-wrap: wrap; gap: 7px; padding: 9px; }
        .divider { align-self: stretch; border-left: 1px solid var(--line); margin: 0 3px; }
        .deploy { align-items: center; display: flex; flex-wrap: wrap; gap: 5px; }
        .deploy button { font-size: 10px; padding: 5px 7px; }
        .deploy button.current { border-color: var(--green); color: var(--green); }
        .deploy button.old { border-color: var(--yellow); color: var(--yellow); }
        .primary { background: #0d6670; border-color: var(--cyan); }
        .publish { background: #24583a; border-color: var(--green); }
        .danger { color: var(--red); }
        .quiet { background: transparent; color: var(--muted); }
        .status { color: var(--muted); margin-left: auto; overflow-wrap: anywhere; }
        .status.ok { color: var(--green); }
        .status.error { color: var(--red); }
        .metadata { background: var(--panel); border: 1px solid var(--line); border-top: 0; display: grid; gap: 9px; grid-template-columns: 1.2fr 1fr 1fr; padding: 10px; }
        label { display: grid; gap: 4px; min-width: 0; }
        label > span { color: var(--muted); font-size: 10px; letter-spacing: .08em; text-transform: uppercase; }
        .workspace { display: grid; gap: 12px; grid-template-columns: 260px minmax(0, 1fr); margin-top: 12px; }
        .panel { background: var(--panel); border: 1px solid var(--line); min-width: 0; }
        .panel-head { align-items: center; border-bottom: 1px solid var(--line); display: flex; gap: 8px; justify-content: space-between; min-height: 42px; padding: 8px 11px; }
        .panel-body { padding: 11px; }
        .state-tree { list-style: none; margin: 0; padding: 0; }
        .state-tree .state-tree { border-left: 1px solid #273343; margin-left: 9px; padding-left: 9px; }
        .state-node { display: flex; margin: 3px 0; min-width: 0; width: 100%; }
        .state-node button { background: transparent; border-color: transparent; overflow: hidden; padding: 6px 7px; text-align: left; text-overflow: ellipsis; white-space: nowrap; width: 100%; }
        .state-node button:hover, .state-node button.selected { background: #132433; border-color: var(--cyan); color: var(--cyan); }
        .state-kind { color: var(--muted); font-size: 9px; margin-left: 5px; text-transform: uppercase; }
        .state-fields { display: grid; gap: 9px; grid-template-columns: 1.3fr 1fr 1fr; }
        .section-title { align-items: center; border-bottom: 1px solid #27313e; display: flex; justify-content: space-between; margin: 18px 0 9px; padding-bottom: 7px; }
        .transition { background: #0a0f16; border: 1px solid #2c3745; margin: 8px 0; }
        .transition-head { align-items: center; background: #101823; cursor: pointer; display: flex; gap: 7px; list-style: none; padding: 9px; }
        .transition-head::-webkit-details-marker { display: none; }
        .transition[open] .transition-head { border-bottom: 1px solid #2c3745; }
        .transition-head strong { color: var(--cyan); font-size: 11px; margin-right: auto; }
        .transition-head strong span { color: #d8e1eb; font-weight: 400; margin-left: 9px; }
        .transition-head button { font-size: 10px; padding: 3px 6px; }
        .transition-grid { display: grid; gap: 8px; grid-template-columns: 1.4fr 1.2fr .7fr; padding: 9px; }
        .logic-row { border-top: 1px solid #27313e; padding: 9px; }
        .logic-row > .label { display: block; margin-bottom: 6px; }
        .capability { align-items: end; display: grid; gap: 7px; grid-template-columns: minmax(180px, 1.4fr) repeat(2, minmax(90px, .7fr)) auto; }
        .capability p { color: var(--muted); font-size: 10px; grid-column: 1 / -1; margin: -2px 0 3px; }
        .action-row { border-left: 2px solid #28586a; margin: 5px 0; padding-left: 8px; }
        .empty { border: 1px dashed #2c3745; color: var(--muted); padding: 11px; text-align: center; }
        .capability-map { display: grid; gap: 7px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
        .capability-card { border: 1px solid #283544; padding: 8px; }
        .capability-card code { color: var(--cyan); }
        .capability-card p { color: var(--muted); font-size: 10px; margin: 4px 0 0; }
        details.advanced { border-top: 1px solid var(--line); margin-top: 16px; padding-top: 10px; }
        details.advanced summary { color: var(--muted); cursor: pointer; font-size: 10px; letter-spacing: .1em; text-transform: uppercase; }
        #raw-definition { font-size: 11px; height: 300px; margin-top: 9px; resize: vertical; }
        .raw-actions { display: flex; gap: 7px; margin-top: 7px; }
        @media (max-width: 850px) {
          bot-chart-editor { padding: 10px; }
          .top { align-items: start; flex-direction: column; }
          .revision { max-width: 100%; text-align: left; width: 100%; }
          .metadata, .workspace, .state-fields, .transition-grid { grid-template-columns: minmax(0, 1fr); }
          .status { flex-basis: 100%; margin-left: 0; }
          .divider { display: none; }
          .capability { grid-template-columns: minmax(0, 1fr); }
        }
      </style>
      <script type="importmap">{{ importMap("/scripts/") }}</script>
      <script type="module" src="/scripts/index.js"></script>
    </head>
    <body>
      <BotChartEditor></BotChartEditor>
    </body>
  </html>
</template>
