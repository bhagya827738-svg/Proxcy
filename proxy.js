const CLIENT_AGENT = "claude-cli/1.0.0 (external, cli)";
const DEFAULT_VER = "2023-06-01";
const BASE_ORIGIN = "https://agentrouter.org";

export default {
  async fetch(req, env) {
    if (req.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "*",
        },
      });
    }

    const loc = new URL(req.url);

    if ((req.method === "GET" || req.method === "HEAD") && isLandingPath(loc.pathname)) {
      const page = renderLandingPage(loc.origin, env?.UPSTREAM_ORIGIN || BASE_ORIGIN);
      return new Response(req.method === "HEAD" ? null : page, {
        status: 200,
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store",
          "X-Content-Type-Options": "nosniff",
        },
      });
    }

    if ((req.method === "GET" || req.method === "HEAD") && loc.pathname === "/api/hello") {
      return makeJsonRes({ ok: true, service: "ccc-proxy" }, 200, req.method === "HEAD");
    }

    if ((req.method === "GET" || req.method === "HEAD") && loc.pathname === "/sw.js") {
      return new Response(null, { status: 404, headers: { "Cache-Control": "no-store" } });
    }

    let route = loc.pathname;
    if (!route.startsWith("/v1") && !route.startsWith("/api")) {
      route = "/v1" + (route.startsWith("/") ? route : "/" + route);
    }
    if (route === "/v1/" || route === "/v1") {
      route = "/v1/chat/completions";
    }

    const host = env?.UPSTREAM_ORIGIN || BASE_ORIGIN;
    const dest = new URL(route, host);
    dest.search = loc.search;

    const token = extractKey(req, env);
    if (!token && req.method !== "OPTIONS") {
      return makeJsonRes({ error: "Missing API key authorization." }, 401);
    }

    let isStreamed = false;
    let payloadModel = undefined;
    let bodyData = undefined;

    if (req.method !== "GET" && req.method !== "HEAD") {
      try {
        let raw = await req.text();
        raw = cleanText(raw);
        raw = raw.replace(/"properties"\s*:\s*\[\s*\]/g, '"properties":{}');
        try {
          let parsed = JSON.parse(raw);
          if (parsed && typeof parsed === "object") {
            parsed = sanitizeData(parsed);
            isStreamed = Boolean(parsed.stream === true);
            parsed.stream = true;
            payloadModel = parsed.model;

            if ("temperature" in parsed) delete parsed.temperature;
            if ("top_p" in parsed) delete parsed.top_p;

            if (Array.isArray(parsed.messages)) {
              const knownCalls = new Set();
              parsed.messages = parsed.messages.filter((msg) => {
                if (msg?.role === "assistant" && Array.isArray(msg.tool_calls)) {
                  for (const call of msg.tool_calls) {
                    if (call?.id) knownCalls.add(call.id);
                  }
                }
                if (msg?.role === "tool" || msg?.role === "function") {
                  const refId = msg.tool_call_id || msg.call_id;
                  return Boolean(refId && knownCalls.has(refId));
                }
                return true;
              });
            }

            raw = JSON.stringify(parsed);
            raw = cleanText(raw);
          }
        } catch {}
        bodyData = raw;
      } catch {
        bodyData = req.body;
      }
    }

    const headers = makeUpstreamHeaders(req, token);

    let upstream = null;
    let upstreamBody = null;
    let errPayload = "";
    const maxTries = 3;

    for (let tryCount = 1; tryCount <= maxTries; tryCount++) {
      upstreamBody = null;
      errPayload = "";
      try {
        upstream = await fetch(dest.toString(), {
          method: req.method,
          headers: headers,
          body: bodyData,
        });

        if (!upstream.ok || !upstream.body) {
          errPayload = await upstream.text().catch(() => "");
          if (tryCount < maxTries && isRetryableUpstreamFailure(upstream.status, errPayload)) {
            await delay(300 * tryCount);
            continue;
          }
          break;
        }

        // Peek at the head of the stream to catch upstream errors that arrive
        // with a 200 status. The sampled bytes are replayed, never consumed.
        const peeked = await peekStream(upstream.body);
        if (chkErr(upstream.status, peeked.sample)) {
          errPayload = peeked.sample;
          await peeked.discard();
          if (tryCount < maxTries && isRetryableUpstreamFailure(upstream.status, errPayload)) {
            await delay(300 * tryCount);
            continue;
          }
          break;
        }

        upstreamBody = peeked.body;
        break;
      } catch (err) {
        if (tryCount >= maxTries) {
          errPayload = err?.message || String(err);
          break;
        }
        await delay(800);
      }
    }

    const isMessages = route === "/v1/messages";

    if (!upstream || !upstream.ok || !upstreamBody || chkErr(upstream ? upstream.status : 502, errPayload)) {
      return makeErrResponse(isStreamed, errPayload, payloadModel, isMessages, upstream ? upstream.status : 502);
    }

    if (!isStreamed) {
      // The upstream call is always forced to stream, so fold the SSE back into
      // a normal body — in the shape the caller's API actually speaks.
      return isMessages
        ? collectAnthropicMessage(upstreamBody, upstream.status, payloadModel)
        : collectSseResponse(upstreamBody, upstream.status, payloadModel);
    }

    const upstreamContentType = upstream.headers.get("content-type");
    const makeStreamResponse = (body, status, contentType) => {
      const outputHeaders = new Headers({
        "Cache-Control": "no-store",
        "Access-Control-Allow-Origin": "*",
      });
      if (contentType) outputHeaders.set("content-type", contentType);

      return new Response(cleanStream(body), {
        status: status,
        headers: outputHeaders,
      });
    };

    const initialResponse = makeStreamResponse(upstreamBody, upstream.status, upstreamContentType);
    if (!isMessages) return initialResponse;

    return prepareAnthropicSseResponse(initialResponse, async () => {
      const retried = await fetch(dest.toString(), {
        method: req.method,
        headers: headers,
        body: bodyData,
      });
      if (!retried.ok || !retried.body) {
        return makeJsonRes({ error: "Upstream retry failed." }, retried.status || 502);
      }
      return makeStreamResponse(retried.body, retried.status, retried.headers.get("content-type"));
    });
  },
};

async function prepareAnthropicSseResponse(initialResponse, retryRequest) {
  let response = initialResponse;

  for (let attempt = 0; attempt < 2; attempt++) {
    if (!isSseResponse(response)) {
      if (attempt === 0) return response;
      break;
    }

    const prepared = await readUntilMessageStart(response.body);
    if (prepared) {
      return new Response(streamPreparedBody(prepared), {
        status: response.status,
        headers: response.headers,
      });
    }

    if (attempt === 0 && retryRequest) {
      try {
        response = await retryRequest();
      } catch {
        break;
      }
      continue;
    }
    break;
  }

  return new Response(
    JSON.stringify({
      error: {
        type: "api_error",
        message: "Upstream SSE ended before message_start.",
      },
    }),
    {
      status: 502,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store",
      },
    },
  );
}

function isSseResponse(response) {
  return response?.body && response.headers.get("content-type")?.toLowerCase().includes("text/event-stream");
}

async function readUntilMessageStart(body) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let pending = "";
  let prefix = "";
  let removedNullEvents = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        await reader.cancel().catch(() => {});
        return null;
      }

      pending += decoder.decode(value, { stream: true });
      while (true) {
        const boundary = findEventBoundary(pending);
        if (!boundary) break;

        const event = pending.slice(0, boundary.end);
        pending = pending.slice(boundary.end);
        if (getEventData(event)?.trim() === "null") {
          removedNullEvents++;
          continue;
        }

        prefix += event;
        if (isMessageStartEvent(event)) {
          return { reader, prefix, pending, removedNullEvents, encoder };
        }
      }
    }
  } catch {
    await reader.cancel().catch(() => {});
    return null;
  }
}

function streamPreparedBody(prepared) {
  const decoder = new TextDecoder();
  return new ReadableStream({
    async start(controller) {
      let pending = prepared.pending;
      controller.enqueue(prepared.encoder.encode(prepared.prefix));

      try {
        while (true) {
          const { done, value } = await prepared.reader.read();
          if (done) {
            pending += decoder.decode();
            const filtered = filterCompleteSseEvents(pending, true, prepared.encoder);
            if (filtered.output) controller.enqueue(filtered.output);
            controller.close();
            return;
          }

          pending += decoder.decode(value, { stream: true });
          const filtered = filterCompleteSseEvents(pending, false, prepared.encoder);
          pending = filtered.pending;
          if (filtered.output) controller.enqueue(filtered.output);
        }
      } catch (error) {
        controller.error(error);
      }
    },
    cancel(reason) {
      return prepared.reader.cancel(reason);
    },
  });
}

function filterCompleteSseEvents(input, final, encoder) {
  let pending = input;
  let outputText = "";
  let removedNullEvents = 0;

  while (true) {
    const boundary = findEventBoundary(pending);
    if (!boundary) break;

    const event = pending.slice(0, boundary.end);
    pending = pending.slice(boundary.end);
    if (getEventData(event)?.trim() === "null") {
      removedNullEvents++;
    } else {
      outputText += event;
    }
  }

  if (final) {
    outputText += pending;
    pending = "";
  }

  return {
    output: outputText ? encoder.encode(outputText) : null,
    pending,
    removedNullEvents,
  };
}

function findEventBoundary(value) {
  const match = /(?:\r\n|\n|\r)(?:\r\n|\n|\r)/.exec(value);
  return match ? { end: match.index + match[0].length } : null;
}

function getEventData(event) {
  const values = [];
  for (const line of event.split(/\r\n|\n|\r/)) {
    if (!line.startsWith("data:")) continue;
    const value = line.slice(5);
    values.push(value.startsWith(" ") ? value.slice(1) : value);
  }
  return values.length ? values.join("\n") : null;
}

function isMessageStartEvent(event) {
  for (const line of event.split(/\r\n|\n|\r/)) {
    if (line.slice(0, 6) === "event:" && line.slice(6).trim() === "message_start") return true;
  }

  const data = getEventData(event);
  if (!data) return false;
  try {
    return JSON.parse(data)?.type === "message_start";
  } catch {
    return false;
  }
}

// Reads the first chunk of a stream to inspect it, then hands back a stream
// that still starts with those bytes. Nothing is lost from the response body.
async function peekStream(body) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let first = null;
  let sample = "";

  try {
    const { done, value } = await reader.read();
    if (!done && value) {
      first = value;
      sample = decoder.decode(value, { stream: true }).slice(0, 2048);
    }
  } catch {
    sample = "";
  }

  const discard = () => reader.cancel().catch(() => {});

  const replayed = new ReadableStream({
    async pull(controller) {
      if (first) {
        controller.enqueue(first);
        first = null;
        return;
      }
      try {
        const { done, value } = await reader.read();
        if (done) {
          controller.close();
          return;
        }
        controller.enqueue(value);
      } catch (err) {
        controller.error(err);
      }
    },
    cancel(reason) {
      first = null;
      return reader.cancel(reason);
    },
  });

  return { sample, body: replayed, discard };
}

// ------------------------------------------------------------- setup page

function isLandingPath(pathname) {
  const p = (pathname || "/").replace(/\/+$/, "") || "/";
  return p === "/" || p === "/index.html";
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// A single self-describing page. The base URL comes from the incoming request,
// so the same worker prints http://localhost:8787 under `wrangler dev` and the
// real workers.dev origin once deployed. No key is ever read or echoed here.
function renderLandingPage(origin, upstream) {
  const base = escapeHtml(origin);
  const up = escapeHtml(upstream || "");

  const snippets = [
    {
      id: "claude",
      label: "Claude Code",
      note: "Point the CLI at this proxy, then run it as usual.",
      code:
        'export ANTHROPIC_BASE_URL="' + origin + '"\n' +
        'export ANTHROPIC_AUTH_TOKEN="sk-your-key"\n\n' +
        "claude",
    },
    {
      id: "openai",
      label: "OpenAI-compatible",
      note: "For VS Code extensions and any OpenAI SDK client.",
      code:
        'base_url = "' + origin + '/v1"\n' +
        'api_key  = "sk-your-key"\n' +
        'model    = "claude-opus-5"',
    },
    {
      id: "curl",
      label: "curl",
      note: "One request to confirm the proxy is answering.",
      code:
        "curl " + origin + "/v1/messages \\\n" +
        '  -H "x-api-key: sk-your-key" \\\n' +
        '  -H "anthropic-version: 2023-06-01" \\\n' +
        '  -H "content-type: application/json" \\\n' +
        "  -d '" +
        '{"model":"claude-opus-5","max_tokens":64,' +
        '"messages":[{"role":"user","content":"hi"}]}' +
        "'",
    },
  ];

  const tabs = snippets
    .map(
      (s, i) =>
        '<button class="tab' + (i === 0 ? " on" : "") + '" role="tab" data-pane="' +
        s.id + '" aria-selected="' + (i === 0 ? "true" : "false") + '">' +
        escapeHtml(s.label) + "</button>",
    )
    .join("");

  const panes = snippets
    .map(
      (s, i) =>
        '<section class="pane' + (i === 0 ? " on" : "") + '" id="pane-' + s.id + '" role="tabpanel">' +
        '<p class="note">' + escapeHtml(s.note) + "</p>" +
        '<div class="code"><pre>' + escapeHtml(s.code) + "</pre>" +
        '<button class="copy" data-copy="' + escapeHtml(s.code) + '">Copy</button></div>' +
        "</section>",
    )
    .join("");

  const endpoints = [
    ["POST", "/v1/messages", "Anthropic Messages"],
    ["POST", "/v1/chat/completions", "OpenAI Chat Completions"],
    ["POST", "/v1/completions", "OpenAI Completions"],
    ["GET", "/v1/models", "Available models"],
  ]
    .map(
      (r) =>
        '<li><span class="verb v' + r[0] + '">' + r[0] + "</span>" +
        '<code>' + escapeHtml(r[1]) + "</code>" +
        '<span class="desc">' + escapeHtml(r[2]) + "</span></li>",
    )
    .join("");

  return '<!doctype html><html lang="en"><head>' +
    '<meta charset="utf-8" />' +
    '<meta name="viewport" content="width=device-width, initial-scale=1" />' +
    "<title>CCC Proxy</title>" +
    '<link rel="icon" href="data:;base64,iVBORw0KGgo=" />' +
    '<link rel="preconnect" href="https://fonts.googleapis.com" />' +
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />' +
    '<link href="https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400;500;600&family=Martian+Mono:wght@300;400;500&display=swap" rel="stylesheet" />' +
    "<style>" + LANDING_CSS + "</style>" +
    "</head><body>" +
    '<main class="wrap">' +
    '<header class="head">' +
    '<div class="mark"><span class="beacon"></span><span class="word"><b>CCC</b><span>proxy</span></span></div>' +
    '<span class="pill">ready</span>' +
    "</header>" +
    '<p class="lede">This worker is running. Point a client at the base URL below — it forwards to ' +
    (up ? '<code class="up">' + up + "</code>" : "the configured upstream") +
    " with Claude Code headers attached.</p>" +
    '<div class="field"><span class="flabel">Base URL</span>' +
    '<div class="urlrow"><code id="base">' + base + "</code>" +
    '<button class="copy" data-copy="' + base + '">Copy</button></div></div>' +
    '<div class="tabs" role="tablist">' + tabs + "</div>" +
    panes +
    '<div class="field"><span class="flabel">Endpoints</span><ul class="eps">' + endpoints + "</ul></div>" +
    '<p class="foot">Your API key is never stored or sent anywhere by this page. ' +
    "Snippets use a placeholder — substitute your own key.</p>" +
    "</main>" +
    "<script>" + LANDING_JS + "<\/script>" +
    "</body></html>";
}

const LANDING_CSS = [
  ":root{--paper:#f6f7f9;--card:#fff;--sunk:#fafbfc;--ink:#0b0f1a;--slate:#616b80;--faint:#98a1b3;",
  "--line:#e4e8ee;--line-hard:#d7dde7;--signal:#3b4fe4;--signal-soft:#eef0ff;--signal-line:#c8cffb;",
  "--ui:'Instrument Sans',ui-sans-serif,system-ui,sans-serif;--readout:'Martian Mono',ui-monospace,monospace;",
  "--r-sm:7px;--r-md:10px;--r-lg:14px;--ease:cubic-bezier(.22,.8,.28,1);color-scheme:light}",
  "@media(prefers-color-scheme:dark){:root{--paper:#000;--card:#111;--sunk:#181818;--ink:#f5f5f5;",
  "--slate:#a3a3a3;--faint:#737373;--line:#262626;--line-hard:#404040;--signal:#fff;--signal-soft:#262626;",
  "--signal-line:#404040;color-scheme:dark}}",
  "*{box-sizing:border-box}",
  "body{margin:0;background:var(--paper);color:var(--ink);font-family:var(--ui);",
  "font-size:14px;line-height:1.55;-webkit-font-smoothing:antialiased}",
  ".wrap{max-width:640px;margin:0 auto;padding:56px 20px 72px}",
  ".head{display:flex;align-items:center;gap:10px;margin-bottom:22px}",
  ".mark{display:flex;align-items:center;gap:9px}",
  ".beacon{width:8px;height:8px;border-radius:50%;background:var(--signal);",
  "box-shadow:0 0 0 4px var(--signal-soft);animation:pulse 2.4s var(--ease) infinite}",
  "@keyframes pulse{0%,100%{opacity:1}50%{opacity:.45}}",
  "@media(prefers-reduced-motion:reduce){.beacon{animation:none}}",
  ".word{font-family:var(--readout);font-size:12px;letter-spacing:.14em;text-transform:uppercase}",
  ".word b{font-weight:500}.word span{color:var(--faint);margin-left:5px}",
  ".pill{margin-left:auto;font-family:var(--readout);font-size:9.5px;letter-spacing:.09em;",
  "text-transform:uppercase;color:var(--signal);background:var(--signal-soft);",
  "border:1px solid var(--signal-line);border-radius:5px;padding:3px 8px}",
  ".lede{color:var(--slate);margin:0 0 28px;font-size:14px}",
  ".lede code.up{font-family:var(--readout);font-size:11.5px;color:var(--ink);",
  "background:var(--sunk);border:1px solid var(--line);border-radius:5px;padding:1px 5px}",
  ".field{margin-bottom:26px}",
  ".flabel{display:block;font-size:12px;font-weight:500;color:var(--slate);margin-bottom:7px}",
  ".urlrow{display:flex;align-items:center;gap:8px;background:var(--card);",
  "border:1px solid var(--line);border-radius:var(--r-md);padding:10px 10px 10px 13px}",
  ".urlrow code{flex:1;min-width:0;font-family:var(--readout);font-size:12.5px;",
  "overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
  ".tabs{display:flex;gap:5px;margin-bottom:12px;flex-wrap:wrap}",
  ".tab{font-family:var(--ui);font-size:12.5px;color:var(--slate);background:transparent;",
  "border:1px solid transparent;border-radius:var(--r-sm);padding:6px 11px;cursor:pointer;",
  "transition:color var(--ease) .18s,background var(--ease) .18s,border-color var(--ease) .18s}",
  ".tab:hover{color:var(--ink);background:var(--card)}",
  ".tab.on{color:var(--ink);background:var(--card);border-color:var(--line);font-weight:500}",
  ".tab:focus-visible,.copy:focus-visible{outline:2px solid var(--signal);outline-offset:2px}",
  ".pane{display:none}.pane.on{display:block}",
  ".note{color:var(--faint);font-size:12.5px;margin:0 0 9px}",
  ".code{position:relative;background:var(--card);border:1px solid var(--line);",
  "border-radius:var(--r-md);margin-bottom:26px}",
  ".code pre{margin:0;padding:14px 15px;overflow-x:auto;font-family:var(--readout);",
  "font-size:12px;line-height:1.75;white-space:pre}",
  ".code .copy{position:absolute;top:9px;right:9px}",
  ".copy{font-family:var(--ui);font-size:11.5px;font-weight:500;color:var(--slate);",
  "background:var(--sunk);border:1px solid var(--line);border-radius:var(--r-sm);",
  "padding:5px 10px;cursor:pointer;flex:none;",
  "transition:color var(--ease) .18s,border-color var(--ease) .18s}",
  ".copy:hover{color:var(--ink);border-color:var(--line-hard)}",
  ".copy.done{color:var(--signal);border-color:var(--signal-line);background:var(--signal-soft)}",
  ".eps{list-style:none;margin:0;padding:0;background:var(--card);",
  "border:1px solid var(--line);border-radius:var(--r-md);overflow:hidden}",
  ".eps li{display:flex;align-items:center;gap:10px;padding:10px 13px;",
  "border-top:1px solid var(--line)}",
  ".eps li:first-child{border-top:0}",
  ".verb{font-family:var(--readout);font-size:9px;letter-spacing:.07em;color:var(--faint);",
  "border:1px solid var(--line);border-radius:4px;padding:2px 5px;flex:none;min-width:38px;text-align:center}",
  ".eps code{font-family:var(--readout);font-size:12px;flex:none}",
  ".desc{color:var(--faint);font-size:12px;margin-left:auto;text-align:right}",
  ".foot{color:var(--faint);font-size:12px;margin:24px 0 0;line-height:1.6}",
  "@media(max-width:560px){.wrap{padding:36px 16px 56px}.desc{display:none}",
  ".eps code{font-size:11.5px}}",
].join("");

const LANDING_JS = [
  "(function(){",
  "var tabs=document.querySelectorAll('.tab');",
  "for(var i=0;i<tabs.length;i++){",
  "tabs[i].addEventListener('click',function(){",
  "var id=this.getAttribute('data-pane');",
  "var all=document.querySelectorAll('.tab');",
  "for(var j=0;j<all.length;j++){",
  "var on=all[j]===this;",
  "all[j].classList.toggle('on',on);",
  "all[j].setAttribute('aria-selected',on?'true':'false');}",
  "var panes=document.querySelectorAll('.pane');",
  "for(var k=0;k<panes.length;k++){",
  "panes[k].classList.toggle('on',panes[k].id==='pane-'+id);}",
  "});}",
  "function fallbackCopy(text){",
  "try{var f=document.createElement('textarea');f.value=text;",
  "f.setAttribute('readonly','');f.style.position='fixed';f.style.opacity='0';",
  "document.body.appendChild(f);f.select();",
  "var ok=document.execCommand('copy');f.remove();return ok;}catch(e){return false;}}",
  "function flash(btn){var was=btn.textContent;btn.textContent='Copied';",
  "btn.classList.add('done');",
  "setTimeout(function(){btn.textContent=was;btn.classList.remove('done');},1300);}",
  "var copies=document.querySelectorAll('.copy');",
  "for(var c=0;c<copies.length;c++){",
  "copies[c].addEventListener('click',function(){",
  "var btn=this,text=this.getAttribute('data-copy');",
  "if(navigator.clipboard&&navigator.clipboard.writeText){",
  "navigator.clipboard.writeText(text).then(function(){flash(btn);},",
  "function(){if(fallbackCopy(text))flash(btn);});",
  "}else if(fallbackCopy(text)){flash(btn);}",
  "});}",
  "})();",
].join("");


// Upstream only accepts Claude Code CLI clients. A real claude-cli/claude-code
// User-Agent is passed through as-is; anything else (VS Code, curl, browsers)
// is replaced with CLIENT_AGENT, or upstream rejects it as an unauthorized
// client.
function pickUpstreamUserAgent(req) {
  const ua = (req.headers.get("User-Agent") || "").trim();
  return /^claude-(cli|code)\//i.test(ua) ? ua : CLIENT_AGENT;
}

function makeUpstreamHeaders(req, apiKey) {
  const headers = new Headers({
    "Content-Type": req.headers.get("Content-Type") || "application/json",
    "Accept-Encoding": "identity",
    "x-api-key": apiKey,
    Authorization: `Bearer ${apiKey}`,
    "anthropic-version": req.headers.get("anthropic-version") || DEFAULT_VER,
    "User-Agent": pickUpstreamUserAgent(req),
  });

  const acceptHeader = req.headers.get("accept");
  if (acceptHeader) headers.set("accept", acceptHeader);

  // Forward the routing headers Claude Code relies on, minus the caller's key.
  for (const [name, value] of req.headers) {
    const lower = name.toLowerCase();
    const isAnthropic = lower.startsWith("anthropic-") && lower !== "anthropic-api-key";
    if (isAnthropic || lower.startsWith("x-stainless-") || lower === "x-app") {
      headers.set(name, value);
    }
  }

  return headers;
}

function extractKey(req, env) {
  const auth = req.headers.get("Authorization") || "";
  const match = auth.match(/^Bearer\s+(.+)$/i);
  if (match?.[1]?.trim()) return match[1].trim();
  const directKey = req.headers.get("x-api-key") || req.headers.get("anthropic-api-key");
  if (directKey?.trim()) return directKey.trim();
  return env?.API_KEY || "";
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function chkErr(status, text) {
  if (status && status !== 200) return true;
  if (!text) return false;
  return (
    text.includes("rate_limit") ||
    text.includes("ChatRateLimited") ||
    text.includes("1102") ||
    text.includes("upstream_provider_rate_limit") ||
    text.includes("server_is_overloaded") ||
    text.includes("service_unavailable_error") ||
    text.includes("server_error") ||
    containsErrorPayload(text)
  );
}

function containsErrorPayload(text) {
  const candidates = [text];
  for (const line of String(text).split(/\r\n|\n|\r/)) {
    if (line.startsWith("data:")) candidates.push(line.slice(5).trim());
  }

  for (const candidate of candidates) {
    try {
      const parsed = JSON.parse(candidate);
      if (parsed?.type === "error" || parsed?.error) return true;
    } catch {}
  }
  return false;
}

function isRetryableUpstreamFailure(status, text = "") {
  return status === 408 || status === 429 || (status >= 500 && status <= 599) || (
    text.includes("rate_limit") ||
    text.includes("ChatRateLimited") ||
    text.includes("1102") ||
    text.includes("upstream_provider_rate_limit") ||
    text.includes("server_is_overloaded") ||
    text.includes("service_unavailable_error")
  );
}

function extractErrText(raw) {
  if (!raw) return "Upstream service was unavailable. Please try again.";
  let str = String(raw).trim();
  try {
    const obj = JSON.parse(str);
    if (obj?.message) return obj.message;
    if (obj?.error?.message) return obj.error.message;
    if (obj?.error && typeof obj.error === "string") return obj.error;
  } catch {}
  if (str.includes("<html") || str.includes("<!DOCTYPE") || str.includes("<HTML")) {
    return "Upstream service error (HTTP HTML response).";
  }
  return str.length > 250 ? str.slice(0, 250) + "..." : str;
}

function makeErrResponse(isStream, rawErr, fallbackModel = "claude-opus-5", isMessages = false, statusCode = 502) {
  const text = extractErrText(rawErr);
  const msg = `[Notice] Request failed: ${text}`;
  const isRate = Boolean(rawErr) && (
    rawErr.includes("rate_limit") ||
    rawErr.includes("ChatRateLimited") ||
    rawErr.includes("1102") ||
    rawErr.includes("upstream_provider_rate_limit")
  );
  const status = isRate ? 429 : (statusCode && statusCode !== 200 ? statusCode : 502);

  // Anthropic clients reject anything that is not {type:"error", error:{...}},
  // and a 200 with an error payload reads to them as a malformed response.
  if (isMessages) {
    const headers = {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
      "Access-Control-Allow-Origin": "*",
    };
    if (isRate) headers["Retry-After"] = "3";
    return new Response(
      JSON.stringify({
        type: "error",
        error: { type: isRate ? "rate_limit_error" : "api_error", message: text },
      }),
      { status, headers },
    );
  }

  if (isStream) {
    const chunk = JSON.stringify({
      id: "chatcmpl-err",
      object: "chat.completion.chunk",
      created: Math.floor(Date.now() / 1000),
      model: fallbackModel || "claude-opus-5",
      choices: [
        {
          index: 0,
          delta: { role: "assistant", content: msg },
          finish_reason: "stop"
        }
      ]
    });
    const sse = `data: ${chunk}\n\ndata: [DONE]\n\n`;
    return new Response(sse, {
      status: 200,
      headers: {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache",
        "Access-Control-Allow-Origin": "*"
      }
    });
  }

  return makeJsonRes({
    id: "chatcmpl-err",
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: fallbackModel || "claude-opus-5",
    choices: [
      {
        index: 0,
        message: { role: "assistant", content: msg },
        finish_reason: "stop"
      }
    ]
  }, 200);
}

function cleanStream(bodyStream) {
  if (!bodyStream) return bodyStream;
  const dec = new TextDecoder();
  const enc = new TextEncoder();
  let lineBuf = "";
  let reader = null;

  return new ReadableStream({
    async start(ctrl) {
      reader = bodyStream.getReader();
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            if (lineBuf.length > 0) {
              const res = filterLine(lineBuf);
              if (!res.skip && res.text) ctrl.enqueue(enc.encode(res.text));
            }
            ctrl.close();
            break;
          }
          lineBuf += dec.decode(value, { stream: true });
          const parts = lineBuf.split("\n");
          lineBuf = parts.pop() || "";
          for (const line of parts) {
            const res = filterLine(line);
            // Blank lines are SSE event terminators — they must be preserved
            // verbatim or downstream parsers never see an event boundary.
            if (!res.skip) ctrl.enqueue(enc.encode((res.text ?? "") + "\n"));
          }
        }
      } catch (err) {
        ctrl.error(err);
      }
    },
    // Propagate client disconnects upstream so the connection is not leaked.
    cancel(reason) {
      return reader ? reader.cancel(reason) : bodyStream.cancel(reason);
    },
  });
}

function filterLine(str) {
  const trimmed = str.trim();
  if (!trimmed) return { skip: false, text: str };

  if (trimmed.includes('"billing.summary"') || trimmed.includes('"object":"billing.summary"')) {
    return { skip: true };
  }
  if (trimmed.includes('"billing":') && !trimmed.includes('"choices":') && !trimmed.includes('"error":')) {
    return { skip: true };
  }
  if (chkErr(200, trimmed)) {
    const cleanMsg = extractErrText(trimmed);
    const item = {
      id: "chatcmpl-err",
      object: "chat.completion.chunk",
      choices: [
        {
          index: 0,
          delta: { content: `\n\n[Notice] Request failed: ${cleanMsg}` },
          finish_reason: "stop"
        }
      ]
    };
    return { skip: false, text: `data: ${JSON.stringify(item)}\n\ndata: [DONE]` };
  }
  return { skip: false, text: str };
}

// Folds a native Anthropic SSE stream back into a single `message` object, the
// same shape a non-streaming /v1/messages call would have returned. Returning
// the OpenAI chat.completion shape here reads to Anthropic clients as an empty
// or malformed HTTP 200.
async function collectAnthropicMessage(body, status, modelFallback) {
  const reader = body.getReader();
  const dec = new TextDecoder();
  let buffer = "";
  let raw = "";
  let sawEvent = false;

  let message = null;
  const blocks = [];
  let stopReason = null;
  let stopSequence = null;
  let usage = null;

  const blockAt = (index) => {
    if (!blocks[index]) blocks[index] = { type: "text", text: "" };
    return blocks[index];
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    const chunk = dec.decode(value, { stream: true });
    raw += chunk;
    buffer += chunk;
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    for (let line of lines) {
      line = line.trim();
      if (!line.startsWith("data:")) continue;
      const jsonStr = line.slice(5).trim();
      if (!jsonStr || jsonStr === "[DONE]") continue;

      let event;
      try {
        event = JSON.parse(jsonStr);
      } catch {
        continue;
      }
      if (!event || typeof event !== "object") continue;
      sawEvent = true;

      switch (event.type) {
        case "message_start":
          if (event.message) {
            message = { ...event.message };
            if (event.message.usage) usage = { ...event.message.usage };
          }
          break;
        case "content_block_start": {
          if (!event.content_block) break;
          const index = event.index ?? blocks.length;
          const started = { ...event.content_block };
          // tool_use arguments arrive later as partial JSON fragments.
          if (started.type === "tool_use" && started.input === undefined) started.input = "";
          blocks[index] = started;
          break;
        }
        case "content_block_delta": {
          const block = blockAt(event.index ?? 0);
          const delta = event.delta || {};
          if (typeof delta.text === "string") block.text = (block.text || "") + delta.text;
          if (typeof delta.partial_json === "string") {
            block.input = (typeof block.input === "string" ? block.input : "") + delta.partial_json;
          }
          if (typeof delta.thinking === "string") block.thinking = (block.thinking || "") + delta.thinking;
          if (typeof delta.signature === "string") block.signature = delta.signature;
          break;
        }
        case "message_delta":
          if (event.delta?.stop_reason !== undefined) stopReason = event.delta.stop_reason;
          if (event.delta?.stop_sequence !== undefined) stopSequence = event.delta.stop_sequence;
          if (event.usage) usage = { ...(usage || {}), ...event.usage };
          break;
        case "error":
          return makeJsonRes(
            { type: "error", error: event.error || { type: "api_error", message: "Upstream error." } },
            502,
          );
        default:
          break;
      }
    }
  }

  const content = blocks.filter(Boolean).map((block) => {
    if (block.type === "tool_use" && typeof block.input === "string") {
      try {
        return { ...block, input: block.input ? JSON.parse(block.input) : {} };
      } catch {
        return { ...block, input: {} };
      }
    }
    return block;
  });

  if (!message) {
    // Upstream may ignore the forced stream flag and answer with a plain
    // Anthropic message body, which is already what the caller wants.
    if (!sawEvent) {
      const trimmed = raw.trim();
      if (trimmed) {
        try {
          const direct = JSON.parse(trimmed);
          if (direct && typeof direct === "object" && (direct.type === "message" || direct.content)) {
            return makeJsonRes(direct, status);
          }
          if (direct && direct.type === "error") return makeJsonRes(direct, 502);
        } catch {}
      }
    }
    return makeJsonRes(
      { type: "error", error: { type: "api_error", message: "Upstream returned no message content." } },
      502,
    );
  }

  return makeJsonRes(
    {
      ...message,
      type: "message",
      role: message.role || "assistant",
      model: message.model || modelFallback || "",
      content,
      stop_reason: stopReason ?? message.stop_reason ?? "end_turn",
      stop_sequence: stopSequence ?? message.stop_sequence ?? null,
      ...(usage ? { usage } : {}),
    },
    status,
  );
}

async function collectSseResponse(body, status, modelFallback) {
  const reader = body.getReader();
  const dec = new TextDecoder();
  let buffer = "";
  let accumText = "";
  let callsMap = {};
  let stopReason = "stop";
  let seqId = "chatcmpl-proxy";
  let targetModel = modelFallback || "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += dec.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    for (let line of lines) {
      line = line.trim();
      if (!line.startsWith("data:")) continue;
      const jsonStr = line.slice(5).trim();
      if (jsonStr === "[DONE]") continue;

      try {
        const item = JSON.parse(jsonStr);
        if (item.id) seqId = item.id;
        if (item.model) targetModel = item.model;
        if (item.type === "content_block_delta" && item.delta?.text) {
          accumText += item.delta.text;
        }
        const itemChoice = item.choices?.[0];
        if (itemChoice) {
          if (itemChoice.finish_reason) stopReason = itemChoice.finish_reason;
          const delta = itemChoice.delta;
          if (delta) {
            if (delta.content) accumText += delta.content;
            if (Array.isArray(delta.tool_calls)) {
              for (const tc of delta.tool_calls) {
                const idx = tc.index ?? 0;
                if (!callsMap[idx]) {
                  callsMap[idx] = { id: tc.id || "", type: tc.type || "function", function: { name: "", arguments: "" } };
                }
                if (tc.id) callsMap[idx].id = tc.id;
                if (tc.function?.name) callsMap[idx].function.name += tc.function.name;
                if (tc.function?.arguments) callsMap[idx].function.arguments += tc.function.arguments;
              }
            }
          }
        }
      } catch {}
    }
  }

  const callsList = Object.values(callsMap);
  const msg = { role: "assistant" };
  if (accumText || !callsList.length) msg.content = accumText;
  if (callsList.length) msg.tool_calls = callsList;

  const result = {
    id: seqId,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: targetModel,
    choices: [
      {
        index: 0,
        message: msg,
        finish_reason: stopReason,
      },
    ],
  };

  return makeJsonRes(result, status);
}

function makeJsonRes(data, status = 200, omitBody = false) {
  return new Response(omitBody ? null : JSON.stringify(sanitizeData(data)), {
    status: status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
    },
  });
}

export { chkErr, isRetryableUpstreamFailure };

function cleanText(txt) {
  if (typeof txt !== "string") return txt;
  let res = typeof txt.toWellFormed === "function" ? txt.toWellFormed() : txt.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "�");
  return res
    .replace(/\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})/g, "\\uFFFD")
    .replace(/(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}/g, "\\uFFFD");
}

function sanitizeData(item) {
  if (typeof item === "string") {
    return cleanText(item);
  }
  if (Array.isArray(item)) {
    return item.map(sanitizeData);
  }
  if (item && typeof item === "object") {
    const out = {};
    for (const k of Object.keys(item)) {
      out[cleanText(k)] = sanitizeData(item[k]);
    }
    return out;
  }
  return item;
}
