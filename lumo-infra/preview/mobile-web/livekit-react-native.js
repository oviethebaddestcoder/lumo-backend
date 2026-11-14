// Preview stub: live video needs the native LiveKit SDK.
const React = require('react');
const { View, Text } = require('react-native');
exports.registerGlobals = () => {};
exports.LiveKitRoom = ({ children }) => React.createElement(View, { style: { flex: 1 } }, children);
exports.VideoTrack = () => React.createElement(View, { style: { flex: 1, backgroundColor: '#111' } });
exports.isTrackReference = () => false;
exports.useTracks = () => [];
exports.useLocalParticipant = () => ({ localParticipant: { setCameraEnabled: async () => {}, setMicrophoneEnabled: async () => {} } });
exports.useRoomContext = () => ({ on() {}, off() {} });
exports.AudioSession = { startAudioSession: async () => {}, stopAudioSession: async () => {} };
exports.PreviewNote = () => React.createElement(Text, null, 'Live video runs on device');
