// Preview stub: keychain storage -> localStorage.
exports.AFTER_FIRST_UNLOCK_THIS_DEVICE_ONLY = 0;
exports.getItemAsync = async (k) => globalThis.localStorage?.getItem(k) ?? null;
exports.setItemAsync = async (k, v) => globalThis.localStorage?.setItem(k, v);
exports.deleteItemAsync = async (k) => globalThis.localStorage?.removeItem(k);
