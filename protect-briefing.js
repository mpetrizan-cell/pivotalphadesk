// ============================================================
// PivotAlphaDesk — Briefing Access Gate
// ============================================================
// INSTALL: Add to <head> of each briefing_*.html:
//   <script src="protect-briefing.js"></script>
// ============================================================
(function(){
'use strict';

// Access codes — add one per user/trial
var ACCESS_CODES = [
  'PAD2026PRO',   // Pro members
  'PADTRIAL'      // Free trial — 7 days
];

// Trial codes with expiration (days from first use)
var TRIAL_CODES = {
  'PADTRIAL': 7
};

var SESSION_HOURS = 12;
var STORAGE_KEY   = 'pad_access_v2';

function isValidCode(code) {
  return ACCESS_CODES.indexOf(code.toUpperCase().trim()) !== -1;
}

function checkAccess() {
  try {
    var stored = localStorage.getItem(STORAGE_KEY);
    if (!stored) return false;
    var data = JSON.parse(stored);
    var code = data.code ? data.code.toUpperCase() : '';

    // Check session expiry
    var sessionExpiry = data.session_expiry || 0;
    if (Date.now() > sessionExpiry) return false;

    // Check if trial code has expired
    if (TRIAL_CODES[code] !== undefined) {
      var trialExpiry = data.trial_expiry || 0;
      if (Date.now() > trialExpiry) {
        localStorage.removeItem(STORAGE_KEY);
        return false;
      }
    }
    return true;
  } catch(e) { return false; }
}

function grantAccess(code) {
  code = code.toUpperCase().trim();
  var now = Date.now();
  var data = {
    code: code,
    session_expiry: now + (SESSION_HOURS * 3600 * 1000)
  };
  // If trial code, set trial expiry
  if (TRIAL_CODES[code] !== undefined) {
    var existing = null;
    try { existing = JSON.parse(localStorage.getItem(STORAGE_KEY)); } catch(e) {}
    var trialStart = (existing && existing.trial_start) ? existing.trial_start : now;
    data.trial_start   = trialStart;
    data.trial_expiry  = trialStart + (TRIAL_CODES[code] * 24 * 3600 * 1000);
  }
  localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
}

function getDaysRemaining(code) {
  try {
    var data = JSON.parse(localStorage.getItem(STORAGE_KEY));
    if (!data || !data.trial_expiry) return null;
    var ms = data.trial_expiry - Date.now();
    return Math.max(0, Math.ceil(ms / (24 * 3600 * 1000)));
  } catch(e) { return null; }
}

function showLockScreen() {
  var style = document.createElement('style');
  style.textContent = [
    'body{margin:0;padding:0;overflow:hidden;}',
    '#pad-gate{position:fixed;inset:0;z-index:99999;background:#070a0e;display:flex;',
    'align-items:center;justify-content:center;font-family:"Space Mono",monospace;}',
    '.pad-gate-box{width:400px;text-align:center;padding:40px 32px;border:1px solid #1e2d3d;',
    'background:#0d1319;}',
    '.pad-gate-logo{font-size:11px;letter-spacing:.2em;color:#00d4ff;text-transform:uppercase;',
    'margin-bottom:8px;}',
    '.pad-gate-title{font-size:22px;font-weight:700;color:#fff;margin-bottom:6px;',
    'letter-spacing:-.02em;}',
    '.pad-gate-sub{font-size:11px;color:#4a6070;letter-spacing:.08em;margin-bottom:32px;}',
    '.pad-gate-input{width:100%;background:#111820;border:1px solid #1e2d3d;color:#e8f4f8;',
    'font-family:"Space Mono",monospace;font-size:13px;letter-spacing:.1em;padding:12px 16px;',
    'text-align:center;text-transform:uppercase;box-sizing:border-box;outline:none;',
    'margin-bottom:12px;}',
    '.pad-gate-input:focus{border-color:#00d4ff;}',
    '.pad-gate-btn{width:100%;background:#00d4ff;color:#070a0e;border:none;padding:12px;',
    'font-family:"Space Mono",monospace;font-size:11px;letter-spacing:.12em;',
    'text-transform:uppercase;font-weight:700;cursor:pointer;margin-bottom:16px;}',
    '.pad-gate-btn:hover{background:#fff;}',
    '.pad-gate-error{font-size:10px;color:#ff4444;letter-spacing:.08em;min-height:16px;',
    'margin-bottom:16px;}',
    '.pad-gate-link{font-size:10px;color:#4a6070;letter-spacing:.08em;}',
    '.pad-gate-link a{color:#00d4ff;text-decoration:none;}',
    '.pad-gate-trial{font-size:9px;color:#f0b429;letter-spacing:.08em;margin-top:12px;}'
  ].join('');
  document.head.appendChild(style);

  var box = document.createElement('div');
  box.id = 'pad-gate';
  box.innerHTML = [
    '<div class="pad-gate-box">',
    '<div class="pad-gate-logo">PivotAlphaDesk</div>',
    '<div class="pad-gate-title">Pro Access Required</div>',
    '<div class="pad-gate-sub">Enter your access code to continue</div>',
    '<input class="pad-gate-input" id="pad-code-input" type="text" ',
    'placeholder="ACCESS CODE" maxlength="20" autocomplete="off" />',
    '<div class="pad-gate-error" id="pad-gate-error"></div>',
    '<button class="pad-gate-btn" id="pad-gate-submit">ACCESS BRIEFING</button>',
    '<div class="pad-gate-link">',
    'Not a member? <a href="pricing.html">Start Free Trial &rarr;</a>',
    '</div>',
    '</div>'
  ].join('');
  document.body.appendChild(box);

  var input  = document.getElementById('pad-code-input');
  var errEl  = document.getElementById('pad-gate-error');
  var btn    = document.getElementById('pad-gate-submit');

  function trySubmit() {
    var code = input.value.toUpperCase().trim();
    if (!code) { errEl.textContent = 'Enter your access code.'; return; }
    if (isValidCode(code)) {
      grantAccess(code);
      var days = getDaysRemaining(code);
      box.remove();
      if (days !== null) {
        var notice = document.createElement('div');
        notice.style.cssText = 'position:fixed;bottom:16px;right:16px;z-index:9999;' +
          'background:#0d1319;border:1px solid #f0b429;color:#f0b429;' +
          'font-family:"Space Mono",monospace;font-size:10px;letter-spacing:.08em;' +
          'padding:8px 14px;border-radius:2px;';
        notice.textContent = 'TRIAL ACCESS — ' + days + ' days remaining';
        document.body.appendChild(notice);
        setTimeout(function(){ notice.remove(); }, 5000);
      }
    } else {
      errEl.textContent = 'Invalid code. Try again.';
      input.value = '';
      input.focus();
    }
  }

  btn.addEventListener('click', trySubmit);
  input.addEventListener('keydown', function(e){
    if (e.key === 'Enter') trySubmit();
  });
  input.focus();
}

// Run
if (!checkAccess()) {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', showLockScreen);
  } else {
    showLockScreen();
  }
}
})();
