// Offline support. The deploy workflow replaces 48cff7b7e85d with the commit SHA,
// so every release gets a fresh cache and old caches are deleted on activate.
const CACHE = 'math3-adventure-48cff7b7e85d';
const SHELL = ['./', 'index.html', 'manifest.webmanifest', 'icon.svg', 'icon-192.png', 'icon-512.png'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k.startsWith('math3-adventure-') && k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

const put = (req, res) => {
  if (res && res.ok) { const copy = res.clone(); caches.open(CACHE).then(c => c.put(req, copy)); }
  return res;
};

self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  const sameOrigin = url.origin === self.location.origin;
  const isFont = url.hostname === 'fonts.googleapis.com' || url.hostname === 'fonts.gstatic.com';
  if (!sameOrigin && !isFont) return;

  // Pages: network first so a new release shows up right away; cache when offline.
  if (req.mode === 'navigate' || url.pathname.endsWith('.html')) {
    e.respondWith(fetch(req).then(res => put(req, res)).catch(() => caches.match(req).then(hit => hit || caches.match('index.html'))));
    return;
  }
  // Icons, manifest, fonts: cache first.
  e.respondWith(caches.match(req).then(hit => hit || fetch(req).then(res => put(req, res))));
});
