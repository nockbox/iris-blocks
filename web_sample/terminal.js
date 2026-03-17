import init, * as iris_wasm from 'https://esm.sh/@nockbox/iris-wasm@0.2.0-alpha.7/iris_wasm.js';

await init();

/**
 * Terminal — a minimal REPL that supports SQL and JavaScript modes,
 * with a pluggable command system.
 *
 * Usage:
 *   const term = new Terminal({
 *     outputEl:  document.getElementById('terminal-output'),
 *     inputEl:   document.getElementById('terminal-input'),
 *     promptEl:  document.querySelector('#terminal-input-bar span'),
 *     sendQuery: (sql) => workerSendQuery(sql),
 *   });
 */
export default class Terminal {
  /** @type {'sql'|'js'} */
  mode = 'sql';

  /** Persistent JS variable context — survives mode switches and worker restarts. */
  jsContext = {};

  /** Last SQL query result (parsed object), accessible from JS mode as `lastResult`. */
  lastResult = undefined;

  // ---- internal state ----
  #outputEl;
  #inputEl;
  #promptEl;
  #sendQuery;
  #history = [];
  #historyIndex = -1;
  #queryBuffer = [];
  #ghostEl;       // grayed-out completion suffix
  #ghostMirror;   // invisible span mirroring typed text (for alignment)

  /** @type {Object<string, { description: string, handler: (args: string) => void }>} */
  commands = {};

  // ---- prompts ----
  static PROMPTS = {
    sql: 'nocksql>',
    js: 'iris>',
    continuation: '   ...>',
  };

  /**
   * @param {{ outputEl: HTMLElement, inputEl: HTMLInputElement, promptEl: HTMLElement, sendQuery: (sql: string) => Promise<string> }} opts
   */
  constructor({ outputEl, inputEl, promptEl, sendQuery }) {
    this.#outputEl = outputEl;
    this.#inputEl = inputEl;
    this.#promptEl = promptEl;
    this.#sendQuery = async (q) => JSON.parse(await sendQuery(q));

    this.#setupGhostOverlay();
    this.#registerBuiltinCommands();
    this.#bindEvents();
    this.#setPrompt(Terminal.PROMPTS.sql);
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Print a line to the terminal output. */
  appendOutput(text, color = '#abb2bf') {
    const div = document.createElement('div');
    div.className = 'output-line';
    div.style.color = color;
    div.style.marginBottom = '2px';
    div.style.whiteSpace = 'pre';
    div.textContent = text;
    this.#outputEl.appendChild(div);
    this.#outputEl.scrollTop = this.#outputEl.scrollHeight;
  }

  /** Replace the sendQuery callback (e.g. after a worker restart). */
  setQueryFn(fn) {
    this.#sendQuery = fn;
  }

  // ---------------------------------------------------------------------------
  // Commands
  // ---------------------------------------------------------------------------

  #registerBuiltinCommands() {
    this.commands['.help'] = {
      description: 'Show available commands',
      handler: () => {
        const lines = Object.entries(this.commands)
          .map(([name, cmd]) => `  ${name.padEnd(10)} ${cmd.description}`)
          .join('\n');
        this.appendOutput(lines, '#c678dd');
      },
    };

    this.commands['.sql'] = {
      description: 'Switch to SQL mode, or .sql QUERY to run inline',
      handler: async (args) => {
        if (args) {
          // Execute inline query without switching mode
          const sql = args.endsWith(';') ? args : args + ';';
          try {
            const result = await this.#sendQuery(sql);
            this.lastResult = result;
            this.appendOutput(Terminal.renderTable(result), '#98be65');
          } catch (err) {
            this.appendOutput(`Error: ${err}`, '#ff6c6b');
          }
        } else {
          this.mode = 'sql';
          this.#setPrompt(Terminal.PROMPTS.sql);
          this.appendOutput('Switched to SQL mode.', '#c678dd');
        }
      },
    };

    this.commands['.js'] = {
      description: 'Switch to JS mode, or .js EXPR to run inline',
      handler: async (args) => {
        if (args) {
          // Execute inline JS without switching mode
          try {
            const result = await this.#evalJs(args);
            if (result !== undefined) {
              this.appendOutput(Terminal.formatJsValue(result), '#98be65');
            }
          } catch (err) {
            this.appendOutput(`Error: ${err}`, '#ff6c6b');
          }
        } else {
          this.mode = 'js';
          this.#setPrompt(Terminal.PROMPTS.js);
          this.appendOutput('Switched to JavaScript mode. The following globals are injected:', '#c678dd');
          this.appendOutput('lastQuery - result of the last query ran in SQL mode', '#c678dd');
          this.appendOutput('iris - iris-wasm module', '#c678dd');
          this.appendOutput('sqlQuery - function to run a query in javascript', '#c678dd');
          this.appendOutput('nounRows - function to get jam rows from a table and return them as nouns (args: tailExpr, e.g. "blocks where height < 10")', '#c678dd');
        }
      },
    };

    this.appendOutput('Welcome to iris-blocks. Type ".help" for commands.', '#c678dd');
  }

  // ---------------------------------------------------------------------------
  // Input handling
  // ---------------------------------------------------------------------------

  #bindEvents() {
    this.#inputEl.addEventListener('keydown', (e) => this.#onKeydown(e));
    this.#inputEl.addEventListener('input', () => this.#updateGhost());
    this.#inputEl.addEventListener('paste', (e) => this.#onPaste(e));
  }

  async #onKeydown(e) {
    if (e.key === 'Tab' && this.mode === 'js') {
      // Accept ghost completion
      const ghost = this.#ghostEl.textContent;
      if (ghost) {
        e.preventDefault();
        this.#inputEl.value += ghost;
        this.#clearGhost();
        return;
      }
    }
    if (e.key === 'Enter') {
      this.#clearGhost();
      await this.#handleEnter();
    } else if (e.key === 'ArrowUp') {
      this.#historyUp();
      e.preventDefault();
    } else if (e.key === 'ArrowDown') {
      this.#historyDown();
      e.preventDefault();
    } else if (e.key === 'Escape' || (e.key === 'c' && e.ctrlKey)) {
      this.#clearGhost();
      this.#cancelBuffer(e);
    }
  }

  async #handleEnter() {
    const line = this.#inputEl.value;
    if (!line.trim() && this.#queryBuffer.length === 0) return;

    const prompt = this.#currentPrompt();
    this.appendOutput(`${prompt} ${line}`, '#51afef');
    this.#inputEl.value = '';

    // Check for dot-commands (only when buffer is empty and line starts with '.')
    if (this.#queryBuffer.length === 0 && line.trim().startsWith('.')) {
      const parts = line.trim().split(/\s+/);
      const cmdName = parts[0];
      const args = parts.slice(1).join(' ');
      const cmd = this.commands[cmdName];
      if (cmd) {
        this.#pushHistory(line.trim());
        cmd.handler(args);
        return;
      } else {
        this.appendOutput(`Unknown command: ${cmdName}. Type .help for available commands.`, '#ff6c6b');
        return;
      }
    }

    if (this.mode === 'sql') {
      await this.#handleSqlInput(line);
    } else {
      await this.#handleJsInput(line);
    }
  }

  // ---- SQL mode ----

  async #handleSqlInput(line) {
    this.#queryBuffer.push(line);
    const full = this.#queryBuffer.join('\n').trim();

    if (full.endsWith(';')) {
      this.#queryBuffer = [];
      this.#setPrompt(Terminal.PROMPTS.sql);
      this.#pushHistory(full);

      try {
        const result = await this.#sendQuery(full);
        this.lastResult = result;
        this.appendOutput(Terminal.renderTable(result), '#98be65');
      } catch (err) {
        this.appendOutput(`Error: ${err}`, '#ff6c6b');
      }
    } else {
      this.#setPrompt(Terminal.PROMPTS.continuation);
    }
  }

  // ---- JS mode ----

  async #handleJsInput(line) {
    this.#queryBuffer.push(line);
    const full = this.#queryBuffer.join('\n').trim();

    // Heuristic: execute immediately unless the line ends with { or is clearly
    // incomplete (unmatched braces).  For simplicity we execute on every Enter
    // unless the user explicitly continues with a trailing backslash or the
    // brace count is unbalanced.
    const open = (full.match(/{/g) || []).length;
    const close = (full.match(/}/g) || []).length;
    if (open > close) {
      // Unbalanced — wait for more input
      this.#setPrompt(Terminal.PROMPTS.continuation);
      return;
    }

    this.#queryBuffer = [];
    this.#setPrompt(Terminal.PROMPTS.js);
    this.#pushHistory(full);

    try {
      const result = await this.#evalJs(full);
      if (result !== undefined) {
        this.appendOutput(Terminal.formatJsValue(result), '#98be65');
      }
    } catch (err) {
      this.appendOutput(`Error: ${err}`, '#ff6c6b');
    }
  }

  /**
   * Evaluate JS code with the persistent jsContext variables in scope.
   * Variable assignments (let/const/var) are captured back into jsContext.
   */
  async #evalJs(code) {
    // Detect variable names to capture: both declarations (let/const/var x =)
    // and bare assignments (x =) at the start of a line.
    const declRe = /^(?:let|const|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=/gm;
    const bareRe = /^([A-Za-z_$][A-Za-z0-9_$]*)\s*=[^=]/gm;
    const newNames = [];
    let m;
    while ((m = declRe.exec(code)) !== null) newNames.push(m[1]);
    while ((m = bareRe.exec(code)) !== null) newNames.push(m[1]);

    // Build scope: existing jsContext + lastResult + placeholders for new vars
    const scope = {
      ...this.jsContext,
      lastResult: this.lastResult,
      iris: iris_wasm,
      sqlQuery: this.#sendQuery,
      nounRows: async (tail) => {
        const rows = await this.#sendQuery(`SELECT jam FROM ${tail}`);
        return rows.map((r) => iris_wasm.cue(r.jam));
      }
    };
    for (const n of newNames) {
      if (!(n in scope)) scope[n] = undefined;
    }

    const allKeys = Object.keys(scope);
    const captureKeys = allKeys.filter(k => k !== 'lastResult');

    // Rewrite let/const/var → plain assignment so the outer-scope `var` is used
    const rewritten = code.replace(
      /^(let|const|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=/gm,
      '$2 ='
    );

    // Build function body: declare all vars in the outer scope, run user code
    // directly, then return captured vars.  We try to auto-return the last
    // expression so that typing e.g. `x` prints its value.
    const lines = rewritten.split('\n');
    const lastLine = lines[lines.length - 1].trim();

    // If the last line looks like a bare expression (not an assignment, block,
    // or control flow), wrap it with __result__ = ...
    const isAssignment = /^[A-Za-z_$][A-Za-z0-9_$]*\s*=[^=]/.test(lastLine);
    const isBlock = /^(if|for|while|switch|try|function|class)\b/.test(lastLine) || lastLine.endsWith('{');
    if (lastLine && !isAssignment && !isBlock && !lastLine.startsWith('//')) {
      lines[lines.length - 1] = `__result__ = ${lines[lines.length - 1]}`;
    }

    const body = [
      ...allKeys.map(k => `  var ${k} = __scope__['${k}'];`),
      `  var __result__;`,
      ...lines.map(l => `  ${l}`),
      `  return { __result__${captureKeys.length ? ', ' + captureKeys.join(', ') : ''} };`,
    ].join('\n');

    const fn = new Function('__scope__', `return (async (__scope__) => {\n${body}\n})(__scope__);`);
    const out = await fn(scope);

    // Write captured variables back into jsContext
    for (const k of captureKeys) {
      if (k in out) this.jsContext[k] = out[k];
    }

    return out.__result__;
  }

  // ---- History ----

  #pushHistory(entry) {
    if (this.#history[this.#history.length - 1] !== entry) {
      this.#history.push(entry);
    }
    this.#historyIndex = this.#history.length;
  }

  #historyUp() {
    if (this.#queryBuffer.length === 0 && this.#historyIndex > 0) {
      this.#historyIndex--;
      this.#inputEl.value = this.#history[this.#historyIndex];
    }
  }

  #historyDown() {
    if (this.#queryBuffer.length === 0) {
      if (this.#historyIndex < this.#history.length - 1) {
        this.#historyIndex++;
        this.#inputEl.value = this.#history[this.#historyIndex];
      } else {
        this.#historyIndex = this.#history.length;
        this.#inputEl.value = '';
      }
    }
  }

  #cancelBuffer(e) {
    if (this.#queryBuffer.length > 0) {
      e.preventDefault();
      this.#queryBuffer = [];
      this.#setPrompt(Terminal.PROMPTS[this.mode]);
      this.#inputEl.value = '';
      this.appendOutput('(cancelled)', '#888');
    }
  }

  // ---- Paste ----

  #onPaste(e) {
    e.preventDefault();
    const pasted = (e.clipboardData || window.clipboardData).getData('text');
    const lines = pasted.split('\n');

    if (this.#inputEl.value) {
      lines[0] = this.#inputEl.value + lines[0];
    }

    for (let i = 0; i < lines.length - 1; i++) {
      const prompt = this.#currentPrompt();
      this.appendOutput(`${prompt} ${lines[i]}`, '#51afef');
      this.#queryBuffer.push(lines[i]);
    }

    this.#inputEl.value = lines[lines.length - 1];

    if (this.#queryBuffer.length > 0) {
      this.#setPrompt(Terminal.PROMPTS.continuation);
    }
  }

  // ---- Ghost autocomplete ----

  #setupGhostOverlay() {
    // Wrap the input in a positioned container so we can overlay ghost text.
    const wrapper = document.createElement('div');
    wrapper.style.cssText = 'position:relative;flex:1;overflow:hidden;';

    this.#inputEl.parentNode.insertBefore(wrapper, this.#inputEl);
    wrapper.appendChild(this.#inputEl);

    // The input needs to fill the wrapper
    this.#inputEl.style.width = '100%';
    this.#inputEl.style.position = 'relative';
    this.#inputEl.style.zIndex = '1';
    this.#inputEl.style.background = 'transparent';

    // Ghost overlay: invisible mirror text + visible suggestion
    const overlay = document.createElement('div');
    overlay.style.cssText = [
      'position:absolute', 'top:0', 'left:0', 'right:0', 'bottom:0',
      'pointer-events:none', 'display:flex', 'align-items:center',
      'font-family:monospace', 'font-size:0.9rem', 'white-space:pre',
      'overflow:hidden',
    ].join(';') + ';';
    wrapper.appendChild(overlay);

    // Mirror span — same text as input, but invisible (for width alignment)
    this.#ghostMirror = document.createElement('span');
    this.#ghostMirror.style.cssText = 'visibility:hidden;';
    overlay.appendChild(this.#ghostMirror);

    // Ghost span — the completion suffix, grayed out
    this.#ghostEl = document.createElement('span');
    this.#ghostEl.style.cssText = 'color:#555;';
    overlay.appendChild(this.#ghostEl);
  }

  #updateGhost() {
    if (this.mode !== 'js') {
      this.#clearGhost();
      return;
    }

    const val = this.#inputEl.value;
    const suffix = this.#findCompletion(val);
    if (suffix) {
      this.#ghostMirror.textContent = val;
      this.#ghostEl.textContent = suffix;
    } else {
      this.#clearGhost();
    }
  }

  #clearGhost() {
    this.#ghostMirror.textContent = '';
    this.#ghostEl.textContent = '';
  }

  /**
   * Find a completion suffix for the current input value.
   * Handles both top-level names (`last` → `Result`) and
   * dot-access chains (`iris.set` → `Logging`).
   */
  #findCompletion(input) {
    // Match a (possibly dot-separated) identifier chain at the end of input
    const chainMatch = input.match(/((?:[A-Za-z_$][A-Za-z0-9_$]*\.)*[A-Za-z_$][A-Za-z0-9_$]*)$/);
    if (!chainMatch) return null;

    const chain = chainMatch[1];
    const parts = chain.split('.');

    // Build the full scope of available names
    const scope = { ...this.jsContext, lastResult: this.lastResult };

    if (parts.length === 1) {
      // Top-level completion
      const prefix = parts[0];
      if (prefix.length < 2) return null;

      const names = [
        ...Object.keys(scope),
        'console', 'JSON', 'Math', 'Object', 'Array', 'String', 'Number',
        'Boolean', 'Date', 'RegExp', 'Map', 'Set', 'Promise',
        'parseInt', 'parseFloat', 'isNaN', 'isFinite',
        'undefined', 'null', 'true', 'false',
        'typeof', 'instanceof', 'function', 'return',
      ];

      const match = this.#bestMatch(prefix, names);
      return match ? match.slice(prefix.length) : null;
    } else {
      // Dot-access completion: resolve the object, then complete the last part
      const objParts = parts.slice(0, -1);
      const propPrefix = parts[parts.length - 1];

      // Resolve the base object by walking the chain
      let obj = scope[objParts[0]];
      if (obj == null) return null;
      for (let i = 1; i < objParts.length; i++) {
        obj = obj[objParts[i]];
        if (obj == null) return null;
      }

      // Get property names from the resolved object
      let propNames;
      try {
        propNames = Object.getOwnPropertyNames(obj);
        // Also include prototype properties for non-plain objects
        const proto = Object.getPrototypeOf(obj);
        if (proto && proto !== Object.prototype) {
          propNames = [...new Set([...propNames, ...Object.getOwnPropertyNames(proto)])];
        }
      } catch {
        return null;
      }

      if (propPrefix.length < 1) return null;
      const match = this.#bestMatch(propPrefix, propNames);
      return match ? match.slice(propPrefix.length) : null;
    }
  }

  /** Find the best matching name for a prefix: prefers exact case, then shortest. */
  #bestMatch(prefix, names) {
    const lowerPrefix = prefix.toLowerCase();
    let best = null;
    for (const name of names) {
      if (name.toLowerCase().startsWith(lowerPrefix) && name !== prefix) {
        if (!best || (name.startsWith(prefix) && !best.startsWith(prefix)) || name.length < best.length) {
          best = name;
        }
      }
    }
    return best;
  }

  // ---- Helpers ----

  #currentPrompt() {
    return this.#queryBuffer.length === 0
      ? Terminal.PROMPTS[this.mode]
      : Terminal.PROMPTS.continuation;
  }

  #setPrompt(text) {
    this.#promptEl.textContent = text;
  }

  // ---------------------------------------------------------------------------
  // Table rendering (static)
  // ---------------------------------------------------------------------------

  /** Format a JS value for REPL display, handling functions and module namespaces. */
  static formatJsValue(val) {
    if (val === undefined) return 'undefined';
    if (val === null) return 'null';
    if (typeof val === 'function') {
      return val.toString().split('\n').length > 3
        ? `[Function: ${val.name || 'anonymous'}]`
        : val.toString();
    }
    if (typeof val !== 'object') return String(val);

    // Use a replacer that preserves functions / classes in the output
    const display = JSON.stringify(val, (key, v) => {
      if (typeof v === 'function') {
        return v.prototype && v.prototype.constructor === v
          ? `[class ${v.name || 'anonymous'}]`
          : `[Function: ${v.name || 'anonymous'}]`;
      }
      return v;
    }, 2);

    // If JSON.stringify returned undefined (e.g. for exotic objects), fall back
    if (display === undefined) {
      const keys = Object.getOwnPropertyNames(val);
      if (keys.length === 0) return '{}';
      const entries = keys.map(k => {
        try { return `  ${k}: ${Terminal.formatJsValue(val[k])}`; }
        catch { return `  ${k}: [error]`; }
      });
      return '{\n' + entries.join(',\n') + '\n}';
    }

    return display;
  }

  static renderTable(data) {
    try {
      if (!Array.isArray(data) || data.length === 0) {
        return '0 rows found';
      }

      const keys = Object.keys(data[0]);
      const colWidths = {};

      keys.forEach(k => colWidths[k] = k.length);
      data.forEach(row => {
        keys.forEach(k => {
          const val = String(row[k] ?? 'NULL');
          if (val.length > colWidths[k]) colWidths[k] = val.length;
        });
      });

      let header = keys.map(k => k.padEnd(colWidths[k])).join(' | ');
      let separator = keys.map(k => '-'.repeat(colWidths[k])).join('-+-');
      let table = header + '\n' + separator + '\n';

      data.forEach(row => {
        table += keys.map(k => String(row[k] ?? 'NULL').padEnd(colWidths[k])).join(' | ') + '\n';
      });

      return table.trimEnd();
    } catch (e) {
      return 'Error formatting table: ' + e.message + '\nRaw: ' + jsonStr;
    }
  }
}
