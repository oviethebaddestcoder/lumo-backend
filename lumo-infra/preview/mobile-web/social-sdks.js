// Preview stub: native social sign-in SDKs (Google, Facebook, LINE, Kakao, Apple).
const fail = async () => { throw new Error('Native sign-in runs on device'); };
const ns = { configure() {}, hasPlayServices: async () => true, signIn: fail, signOut: async () => {}, login: fail, logOut() {}, logInWithPermissions: fail,
  getCurrentAccessToken: async () => null, getAuthenticationTokenIOS: async () => null, isAvailableAsync: async () => false, signInAsync: fail, setup() {} };
module.exports = { ...ns, GoogleSignin: ns, statusCodes: {}, AccessToken: ns, AuthenticationToken: ns, LoginManager: ns, Scope: {}, default: ns,
  AppleAuthenticationScope: {}, AppleAuthenticationButton: () => null, AppleAuthenticationButtonType: {}, AppleAuthenticationButtonStyle: {} };
