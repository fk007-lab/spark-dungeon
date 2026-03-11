/* global editor instance */
let editor;
const MISSION_ID = window.location.pathname.split('/').pop();

function initEditor(starterCode) {
  editor = CodeMirror.fromTextArea(document.getElementById('code-editor'), {
    mode: 'python',
    theme: 'dracula',
    lineNumbers: true,
    indentUnit: 4,
    tabSize: 4,
    indentWithTabs: false,
    extraKeys: {
      'Tab': cm => cm.execCommand('indentMore'),
      'Shift-Tab': cm => cm.execCommand('indentLess'),
      'Ctrl-Enter': () => runCode(),
      'Cmd-Enter': () => runCode(),
    },
  });
  if (starterCode) editor.setValue(starterCode);
}

async function runCode() {
  const btn = document.getElementById('run-btn');
  btn.disabled = true;
  btn.textContent = '⏳ Running…';

  const code = editor.getValue();

  try {
    const resp = await fetch(`/mission/${MISSION_ID}/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code }),
    });
    const data = await resp.json();
    renderResult(data);
  } catch (err) {
    showError('Network error: ' + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = '▶ Run';
  }
}

function renderResult(data) {
  document.getElementById('error-area').style.display = 'none';
  document.getElementById('result-area').style.display = 'none';

  if (data.error && !data.passed && data.result_preview?.length === 0) {
    showError(data.error);
    return;
  }

  // Verdict
  const banner = document.getElementById('verdict-banner');
  if (data.passed) {
    banner.textContent = '✅ Correct! Mission complete.';
    banner.className = 'verdict-banner verdict-pass';
  } else {
    banner.textContent = '❌ Not quite right — keep trying!';
    banner.className = 'verdict-banner verdict-fail';
  }

  // Validation hint (what's wrong with the output)
  const vHint = document.getElementById('validation-hint');
  if (!data.passed && data.validation_hint) {
    vHint.textContent = data.validation_hint;
    vHint.style.display = 'block';
  } else {
    vHint.style.display = 'none';
  }

  // Result table
  renderTable(data.result_preview || []);

  // Explain plan
  if (data.explain_plan) {
    document.getElementById('explain-pre').textContent = data.explain_plan;
    document.getElementById('explain-details').style.display = 'block';
  } else {
    document.getElementById('explain-details').style.display = 'none';
  }

  document.getElementById('result-area').style.display = 'block';

  // Hint (for wrong answer)
  const hintBox = document.getElementById('hint-box');
  if (data.hint && !data.passed) {
    document.getElementById('hint-text').textContent = data.hint;
    hintBox.style.display = 'block';
  } else {
    hintBox.style.display = 'none';
  }

  // Next button on pass
  if (data.passed) {
    document.getElementById('next-btn-wrap').style.display = 'block';
    document.getElementById('next-btn-wrap').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }
}

function renderTable(rows) {
  const table = document.getElementById('result-table');
  table.innerHTML = '';
  if (!rows || rows.length === 0) return;

  const cols = Object.keys(rows[0]);
  const thead = table.createTHead();
  const hrow = thead.insertRow();
  cols.forEach(c => {
    const th = document.createElement('th');
    th.textContent = c;
    hrow.appendChild(th);
  });

  const tbody = table.createTBody();
  rows.forEach(row => {
    const tr = tbody.insertRow();
    cols.forEach(c => {
      const td = tr.insertCell();
      const val = row[c];
      td.textContent = val === null || val === undefined ? 'null' : String(val);
    });
  });
}

function showError(msg) {
  document.getElementById('error-pre').textContent = msg;
  document.getElementById('error-area').style.display = 'block';
}
