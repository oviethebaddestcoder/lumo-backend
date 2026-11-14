// Preview-only Metro config: on web, swap device-only native modules for stubs.
const { getDefaultConfig } = require('expo/metro-config');
const path = require('path');
const config = getDefaultConfig(__dirname);
const stub = (f) => path.join(__dirname, 'preview-stubs', f);
const map = {
  '@livekit/react-native': stub('livekit-react-native.js'),
  '@livekit/react-native-webrtc': stub('empty.js'),
  'react-native-webrtc': stub('empty.js'),
  'expo-secure-store': stub('secure-store.js'),
  '@react-native-firebase/messaging': stub('firebase.js'),
  '@react-native-firebase/app': stub('firebase.js'),
  'expo-notifications': stub('notifications.js'),
  '@react-native-google-signin/google-signin': stub('social-sdks.js'),
  'react-native-fbsdk-next': stub('social-sdks.js'),
  '@xmartlabs/react-native-line': stub('social-sdks.js'),
  '@react-native-seoul/kakao-login': stub('social-sdks.js'),
  'expo-apple-authentication': stub('social-sdks.js'),
};
const prev = config.resolver.resolveRequest;
config.resolver.resolveRequest = (ctx, name, platform) => {
  if (platform === 'web' && map[name]) return { type: 'sourceFile', filePath: map[name] };
  return (prev ?? ctx.resolveRequest)(ctx, name, platform);
};
module.exports = config;
