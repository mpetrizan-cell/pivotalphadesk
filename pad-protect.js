// ============================================================
// PivotAlphaDesk — Content Protection & Index Cleanup
// ============================================================
// INSTALL: Add before </body> in index.html:
//   <script src="pad-protect.js"></script>
// ============================================================
(function(){
'use strict';

// --- 1. SIMPLIFY NAV ---
var navLinks = document.querySelector('.nav-links');
if(navLinks){
  navLinks.innerHTML = `
    <li><a href="#how">How It Works</a></li>
    <li><a href="#features">Features</a></li>
    <li><a href="#pricing">Pricing</a></li>
    <li><a href="pad-library-zonas-algo.html">Library</a></li>
    <li><a href="contact.html">Contact</a></li>
    <li><a href="login.html" class="nav-cta" style="display:flex;align-items:center;gap:6px;">&#128274; Login</a></li>
  `;
}

// --- 2. HERO BUTTONS — Remove direct briefing links ---
var heroActions = document.querySelector('.hero .hero-actions');
if(heroActions){
  heroActions.innerHTML = `
    <a href="#pricing" class="btn-primary" style="background:linear-gradient(135deg,var(--accent),#0088aa);color:#fff;box-shadow:0 0 30px rgba(0,212,255,0.3)">Get Pro Access</a>
    <a href="#how" class="btn-secondary">See How It Works</a>
    <a href="pad-library-zonas-algo.html" class="btn-secondary">&#128218; Free Library</a>
  `;
}

// --- 3. PAD PICK — Hide Entry/Stop/Target ---
var ppfLevels = document.querySelector('.ppf-levels');
if(ppfLevels){
  ppfLevels.innerHTML = `
    <div class="ppf-level" style="justify-content:center;padding:20px 12px;">
      <span style="color:var(--text3);font-family:'Space Mono',monospace;font-size:10px;letter-spacing:.1em;">&#128274; ENTRY / STOP / TARGET</span>
    </div>
    <div class="ppf-level" style="justify-content:center;padding:14px 12px;background:rgba(0,212,255,0.04);">
      <span style="color:var(--accent);font-family:'Space Mono',monospace;font-size:10px;letter-spacing:.08em;">PRO MEMBERS ONLY</span>
    </div>
    <div class="ppf-level" style="justify-content:center;padding:14px 12px;">
      <a href="#pricing" style="color:var(--accent);font-family:'Space Mono',monospace;font-size:10px;letter-spacing:.1em;text-decoration:none;border:1px solid rgba(0,212,255,0.3);padding:8px 20px;border-radius:2px;transition:all .2s;">GET ACCESS &rarr;</a>
    </div>
  `;
}

// Hide verdict (BUY + stop claro en $51)
var ppfVerdict = document.querySelector('.ppf-verdict');
if(ppfVerdict){
  ppfVerdict.style.background = 'rgba(255,255,255,0.02)';
  ppfVerdict.style.border = '1px solid var(--border)';
  ppfVerdict.innerHTML = `
    <div class="ppf-verdict-top">
      <div class="ppf-verdict-rating" style="color:var(--text3);">&#128274;</div>
      <div class="ppf-verdict-meta">Verdict<br>Pro Access</div>
    </div>
    <p style="color:var(--text3);">Full analysis with entry, stop, targets and risk/reward available for Pro members.</p>
  `;
}

// Change "Read Full Analysis" link
var ppfLink = document.querySelector('.ppf-link');
if(ppfLink){
  ppfLink.innerHTML = '&#128274; Full Analysis &mdash; Pro Access &rarr;';
  ppfLink.setAttribute('href','#pricing');
  ppfLink.style.textDecoration = 'none';
}

// Remove the <a> wrapper around pad-pick-featured (make it not clickable to full analysis)
var ppfWrap = document.querySelector('a[href*="pad_pick_semana"]');
if(ppfWrap && ppfWrap.querySelector('.pad-pick-featured')){
  var parent = ppfWrap.parentNode;
  var div = document.createElement('div');
  div.className = ppfWrap.className;
  div.innerHTML = ppfWrap.innerHTML;
  parent.replaceChild(div, ppfWrap);
}

// Change "View All Picks" link to point to pricing
var viewAllPicks = document.querySelector('.pad-pick-all-link');
if(viewAllPicks) viewAllPicks.setAttribute('href','#pricing');

// --- 4. DEEP DESK — Replace descriptions with previews + lock ---
var deepCards = document.querySelectorAll('.deep-card');
deepCards.forEach(function(card){
  // Replace paragraph content with short preview
  var p = card.querySelector('p');
  if(p && p.textContent.length > 80){
    // Extract first sentence or key terms for preview
    var text = p.textContent;
    var preview = text.split('.')[0].substring(0, 60) + '...';
    p.style.color = 'var(--text3)';
    p.style.fontStyle = 'italic';
    p.style.fontSize = '0.9rem';
    p.textContent = preview;
  }
  // Replace all deep-btn links with lock
  var btn = card.querySelector('.deep-btn');
  if(btn && btn.getAttribute('href') !== '#pricing'){
    btn.setAttribute('href','#pricing');
    btn.className = 'deep-btn';
    btn.style.color = 'var(--text3)';
    btn.style.border = '1px solid var(--border)';
    btn.innerHTML = '&#128274; PRO ACCESS REQUIRED';
  }
});

// --- 5. SPX LIVE — Remove fullscreen link ---
var spxLink = document.querySelector('.live-chart-link');
if(spxLink){
  var span = document.createElement('span');
  span.className = 'live-chart-link';
  span.style.cursor = 'default';
  span.style.opacity = '0.5';
  span.innerHTML = '&#128274; Full-Screen View &mdash; Pro Members';
  spxLink.parentNode.replaceChild(span, spxLink);
}

// --- 6. CTA SECTION — Simplify buttons ---
var ctaActions = document.querySelector('.cta-section .hero-actions');
if(ctaActions){
  ctaActions.innerHTML = `
    <a href="#pricing" class="btn-primary" style="background:linear-gradient(135deg,var(--accent),#0088aa);color:#fff;box-shadow:0 0 30px rgba(0,212,255,0.3)">Get Pro Access</a>
    <a href="#how" class="btn-secondary">Learn More</a>
  `;
}

// --- 7. FOOTER — Clean up links ---
var footerLinks = document.querySelector('.footer-inner div[style*="display:flex"]');
if(footerLinks){
  var linkStyle = "font-family:'Space Mono',monospace;font-size:10px;text-decoration:none;letter-spacing:.08em;";
  footerLinks.innerHTML = `
    <a href="pad-library-zonas-algo.html" style="${linkStyle}color:var(--text2);">Library</a>
    <a href="#pricing" style="${linkStyle}color:var(--accent);">Pricing</a>
    <a href="pad_options_calc_v5.html" style="${linkStyle}color:#f0b429;">Options Calculator</a>
    <a href="contact.html" style="${linkStyle}color:var(--text2);">Contact</a>
    <a href="#disclaimer" style="${linkStyle}color:var(--text3);">Disclaimer</a>
  `;
}

// --- 8. LANGUAGE SWITCHER ---
var langDiv = document.createElement('div');
langDiv.id = 'pad-lang-switch';
langDiv.innerHTML = `
  <button onclick="window.location.href='index.html'" class="pad-lang-btn active">ES</button>
  <button onclick="window.location.href='index_en.html'" class="pad-lang-btn">EN</button>
`;
document.body.appendChild(langDiv);

var langCSS = document.createElement('style');
langCSS.textContent = `
  #pad-lang-switch{position:fixed;bottom:24px;right:24px;z-index:999;display:flex;gap:2px;background:var(--bg3);border:1px solid var(--border);border-radius:4px;padding:3px;box-shadow:0 8px 32px rgba(0,0,0,0.6);}
  .pad-lang-btn{font-family:'Space Mono',monospace;font-size:10px;letter-spacing:.08em;padding:6px 14px;border:none;cursor:pointer;border-radius:2px;transition:all .2s;text-transform:uppercase;background:transparent;color:var(--text3);}
  .pad-lang-btn.active{background:var(--accent);color:var(--bg);font-weight:700;}
  .pad-lang-btn:hover:not(.active){color:var(--accent);}
`;
document.head.appendChild(langCSS);

console.log('[PAD] Content protection applied successfully.');
})();
