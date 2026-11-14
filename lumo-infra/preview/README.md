# Visual preview and UI smoke test

Runs the whole product locally and screenshots every screen:
- the backend: all services, with demo data from `e2e/journey/demo_test.go`;
- the **web app**: 13 pages;
- the **mobile app**, rendered in Chrome at phone size with react-native-web: 16 screens.

```bash
WEB_DIR=../lumo-web MOBILE_DIR=../lumo-mobile lumo-infra/preview/run.sh
# screenshots + index.json in /tmp/lumo-preview/shots
```

It fails if any screen throws a JavaScript error or violates the Content-Security-Policy. CI runs it on every push (`preview` job) and attaches the screenshots to the run.

**What the browser preview can't show:** camera, live video (LiveKit), push notifications, Google Play / App Store billing and native sign-in SDKs. `mobile-web/` swaps those for stand-ins, in this preview only; the app repo is not changed. Those features still need testing on real phones.

Its first run caught 4 bugs, all fixed:
- For You went blank once a viewer had seen every video;
- the Live category chips stretched to fill the screen;
- the chat's "Seen" label sat under the wrong message;
- a half-created OBS stream key could be stored and shown.
