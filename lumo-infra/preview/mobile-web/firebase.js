// Preview stub: push notifications are device-only.
const noop = () => () => {};
const messaging = () => ({ getToken: async () => '', onMessage: noop, onTokenRefresh: noop, requestPermission: async () => 0, getInitialNotification: async () => null, onNotificationOpenedApp: noop, setBackgroundMessageHandler: () => {} });
module.exports = messaging;
module.exports.default = messaging;
Object.assign(module.exports, { getMessaging: () => ({}), getToken: async () => '', onMessage: noop, onTokenRefresh: noop, requestPermission: async () => 0,
  getInitialNotification: async () => null, onNotificationOpenedApp: noop, setBackgroundMessageHandler: () => {}, registerDeviceForRemoteMessages: async () => {}, AuthorizationStatus: { AUTHORIZED: 1, PROVISIONAL: 2, DENIED: 0 } });
