self.addEventListener('push', e => {
  const data = e.data ? e.data.json() : {};
  e.waitUntil(self.registration.showNotification(
    data.title || 'NewsPulse',
    {
      body:  data.body || 'New updates available',
      icon:  '/icon.png',
      badge: '/icon.png',
      tag:   'newspulse',
      renotify: true,
    }
  ));
});

self.addEventListener('notificationclick', e => {
  e.notification.close();
  e.waitUntil(clients.openWindow('/'));
});