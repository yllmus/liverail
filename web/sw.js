/* LiveRail service worker - delay alerts.
 *
 * Deliberately tiny. A service worker sits in front of the whole origin and a
 * mistake here can make the site unreachable in a way a reload does not fix,
 * so this one does exactly two things and never touches fetch/caching.
 */
self.addEventListener('install', function(e){ self.skipWaiting(); });
self.addEventListener('activate', function(e){ e.waitUntil(self.clients.claim()); });

self.addEventListener('push', function(e){
  var d = {};
  try { d = e.data ? e.data.json() : {}; } catch (err) { d = {}; }
  var title = d.title || 'Train update';
  e.waitUntil(self.registration.showNotification(title, {
    body: d.body || '',
    // Same tag per watch, so a worsening delay REPLACES the previous
    // notification instead of stacking a pile of them on the lock screen.
    tag: d.tag || 'liverail',
    renotify: true,
    data: { url: d.url || '/' },
    icon: '/icon-192.png',
    badge: '/icon-192.png',
    // A cancellation is the one case worth interrupting for.
    requireInteraction: d.kind === 'cancelled'
  }));
});

self.addEventListener('notificationclick', function(e){
  e.notification.close();
  var url = (e.notification.data && e.notification.data.url) || '/';
  e.waitUntil(clients.matchAll({type:'window', includeUncontrolled:true}).then(function(list){
    for (var i=0;i<list.length;i++){
      // Reuse an already-open tab rather than piling up new ones.
      if (list[i].url.indexOf(self.location.origin) === 0 && 'focus' in list[i]){
        list[i].navigate(url); return list[i].focus();
      }
    }
    return clients.openWindow(url);
  }));
});
