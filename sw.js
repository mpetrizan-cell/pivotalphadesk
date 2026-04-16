// PivotAlphaDesk — GAIA Live Service Worker
const CACHE = 'gaia-v1';
const STATIC = ['/login'];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(STATIC))
  );
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', e => {
  // Para datos live y auth — siempre network first
  if (e.request.url.includes('/gaia_live.json') ||
      e.request.url.includes('/push') ||
      e.request.url.includes('/login') ||
      e.request.url.includes('/logout')) {
    e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
    return;
  }
  // Para assets estáticos — cache first
  e.respondWith(
    caches.match(e.request).then(cached => cached || fetch(e.request))
  );
});
