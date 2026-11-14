// Preview stub: notifications are device-only.
const sub = { remove() {} };
module.exports = {
  setNotificationHandler() {},
  addNotificationResponseReceivedListener: () => sub,
  addNotificationReceivedListener: () => sub,
  getLastNotificationResponse: () => null,
  getLastNotificationResponseAsync: async () => null,
  requestPermissionsAsync: async () => ({ status: 'denied', granted: false }),
  getPermissionsAsync: async () => ({ status: 'denied', granted: false }),
  scheduleNotificationAsync: async () => '',
  setNotificationChannelAsync: async () => {},
  AndroidImportance: { HIGH: 4, DEFAULT: 3 },
};
