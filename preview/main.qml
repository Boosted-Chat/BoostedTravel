import QtQuick
import QtQuick.Window
import qs.Commons
import qs.Ui
import "../Model.js" as Model

// Preview host. Renders the REAL BarWidget.qml and the REAL Panel.qml from the
// plugin root -- neither file is modified or copied for this. Everything they
// import comes from preview/stubs.
//
// The bar strip at the top is the actual bar widget. The surface below is the
// actual panel, held open. `hostWidget` is wired between them exactly as
// BarWidget.injectPanel() does it in the shell, so a completed search updates
// the bar label through the real code path rather than a preview shortcut.
Window {
  id: win
  visible: true
  width: 1180
  height: 1000
  color: Color.background
  title: "LetsFG Flights — Omarchy plugin preview"

  // Stands in for the Omarchy bar. Only the members the plugin actually reads.
  QtObject {
    id: fakeBar
    property color foreground: Color.foreground
    property color barForeground: Color.foreground
    property color urgent: Color.urgent
    property color background: Color.background
    property string fontFamily: Style.font.family
    property bool vertical: false
    property int barSize: 34
    property bool centerHoverRevealSuppressed: false
    property var shell: null
    function switchPanelFrom(identity, direction) { return false }
    function run(cmd) { console.log("[preview] bar.run:", cmd) }
  }

  Column {
    anchors.fill: parent
    spacing: 0

    // ---- the bar -------------------------------------------------------
    Rectangle {
      width: parent.width
      height: 34
      color: "#11121a"

      Text {
        anchors.left: parent.left
        anchors.leftMargin: 12
        anchors.verticalCenter: parent.verticalCenter
        text: "omarchy bar (preview)"
        color: Qt.darker(Color.foreground, 1.9)
        font.family: Style.font.family
        font.pixelSize: 11
        textFormat: Text.PlainText
      }

      Loader {
        id: barLoader
        anchors.right: parent.right
        anchors.rightMargin: 8
        anchors.verticalCenter: parent.verticalCenter
        source: Host.pluginUrl("BarWidget.qml")
        onLoaded: win.wire()
      }
    }

    Rectangle { width: parent.width; height: 1; color: Qt.rgba(1, 1, 1, 0.08) }

    // ---- the panel -----------------------------------------------------
    Item {
      width: parent.width
      height: parent.height - 35

      Loader {
        id: panelLoader
        anchors.fill: parent
        anchors.margins: 16
        source: Host.pluginUrl("Panel.qml")
        onLoaded: win.wire()
      }
    }
  }

  // Rendering the scene graph directly, rather than grabbing the screen at
  // this window's coordinates -- a screen grab captures whatever the window
  // manager has on top, which on a busy desktop is not this window.
  // Both Loaders finish independently and either can be second, so wiring runs
  // from both and only completes once both items exist. Doing it from the
  // panel's onLoaded alone left hostWidget null whenever the bar loaded later
  // -- and hostWidget is what carries the price back to the bar label.
  property bool wired: false

  function wire(): void {
    if (win.wired) return
    if (!barLoader.item || !panelLoader.item) return
    win.wired = true

    var panel = panelLoader.item
    // The same injection BarWidget.injectPanel() performs in the shell.
    barLoader.item.bar = fakeBar
    panel.bar = fakeBar
    panel.anchorItem = barLoader.item
    panel.hostWidget = barLoader.item
    // Route presets arrive through the plugin's real settings path (the
    // shell.json defaults), not by poking at its private field ids.
    var preset = Host.preset()
    if (preset && preset.defaultOrigin) panel.settings = preset
    // Held open: in the shell this is a layer-shell surface toggled by the bar
    // button; here it is simply always on screen.
    panel.open()
  }

  // Render a canned /api/search payload. It still goes through the plugin's own
  // Model.summarizeOffers, so what appears is shaped by the real code -- only
  // the network is skipped. Lets layout work happen without spending searches
  // against a 3-per-10-minutes budget.
  function loadFixture(text: string): void {
    if (!panelLoader.item) return
    var payload = JSON.parse(text)
    // Same path as a live search: build the list the site's way (dedup,
    // drop non-positive prices, price sort, id dedupe), then shape. The
    // display sort is applied afterwards by visibleOffers, as in a real search.
    var shaped = Model.summarizeOffers({ offers: panelLoader.item.prepareRaw(payload.offers) })
    panelLoader.item.offers = shaped
    // Same transition a real search causes: the panel leaves its homepage
    // state and learns the city names from the payload.
    panelLoader.item.hasSearched = true
    // Real offers carry no booking_url, so the card link is built from the
    // search id -- the fixture must supply one or it would exercise a path
    // the live API never takes.
    if (payload.search_id) panelLoader.item.searchId = payload.search_id
    if (shaped.length > 0) {
      if (shaped[0].originName.length > 0) panelLoader.item.originCity = shaped[0].originName
      if (shaped[0].destinationName.length > 0) panelLoader.item.destCity = shaped[0].destinationName
    }
    // AFTER the city names are set: the endpoint needs a destination city and
    // answers "No destination given" without one. The real search path already
    // assigns these before it settles.
    panelLoader.item.fetchTransfers()
    panelLoader.item.statusText = shaped.length + " offers (FIXTURE — no network)"
    if (barLoader.item) barLoader.item.cheapestLabel = Model.cheapestLabel(shaped)
  }

  // Verifies the filter dropdown: it is positioned with mapToItem into an
  // overlay layer, which is the one piece a screenshot can check but a unit
  // test cannot.
  function debugOpenMenu(which: string): void {
    if (!panelLoader.item) return
    // "place:gdan" / "place-to:lond" / "calendar" / a filter name
    var parts = which.split(":")
    panelLoader.item.debugShow(parts[0], parts.length > 1 ? parts[1] : "")
    // The button press lives HERE, not in the plugin: Panel.qml must not
    // contain a way to start a search other than a click.
    if (parts[0] === "hotels-search") panelLoader.item.beginHotelSearch()
  }

  // Stands in for a person pressing Search. This lives in the HARNESS, not in
  // the plugin: Panel.qml still has no way to start a search by itself, which
  // is the invariant tools/validate.sh enforces.
  function simulateSearch(): void {
    if (panelLoader.item) panelLoader.item.beginSearch()
  }

  // Prove the cards are actually clickable: print the link the first offer
  // would open. Empty here means a dead card on screen.
  // First-run check: with no token, does pressing Search actually do nothing?
  // Reports the observable state rather than trusting the guard by eye.
  // Drives the in-panel sign-in and reports the state machine. Note this
  // really does open letsfg.co/connect in a browser -- that is the flow.
  function reportAuth(): void {
    if (!panelLoader.item) return
    var p = panelLoader.item
    console.log("[auth] before: stage=\"" + p.authStage + "\" ready=" + p.tokenStatus.ready)
    p.beginConnect()
    console.log("[auth] after click: stage=\"" + p.authStage + "\"")
  }

  function reportAuthState(): void {
    if (!panelLoader.item) return
    var p = panelLoader.item
    console.log("[auth] settled: stage=\"" + p.authStage + "\" client="
                + (p.authClientId.length > 0 ? p.authClientId.slice(0, 16) + "..." : "(none)")
                + " error=\"" + p.authError + "\"")
  }

  // Fake credentials for the two persistence checks below. They only ever reach
  // the sandbox HOME that run.py creates; nothing sends them to a server.
  readonly property var fakeCredentials: ({
    roundtrip: "letsfg_roundtrip_test_token",
    renewedAccess: "lfg_at_renewed_preview_token",
    rotatedRefresh: "lfg_rt_rotated_preview_token"
  })

  // Round-trip check: write a token the way a real connect would, so the
  // next launch can prove it is remembered.
  function debugSaveToken(): void {
    if (!panelLoader.item) return
    var p = panelLoader.item
    p.tokenStatus = p.session.adoptTokens({
      ok: true, token: fakeCredentials.roundtrip, refreshToken: "",
      expiresAt: Math.floor(Date.now() / 1000) + 80 * 86400
    }, "", "own")
    p.persistSession()
    console.log("[persist] saved; ready=" + p.tokenStatus.ready)
  }

  // Write-back check: a renewal of a token that came from the CLI's file must
  // land in the CLI's file, other keys intact. Adopts as if the token endpoint
  // had just answered, then persists; run.py prints the file afterwards.
  function debugWriteBack(): void {
    if (!panelLoader.item) return
    var p = panelLoader.item
    p.tokenStatus = p.session.adoptTokens({
      ok: true, token: fakeCredentials.renewedAccess, refreshToken: fakeCredentials.rotatedRefresh,
      expiresAt: Math.floor(Date.now() / 1000) + 3600
    }, "lfg_client_preview_0000", "cli")
    p.persistSession()
    console.log("[writeback] persisted to source=" + p.tokenStatus.source + " ready=" + p.tokenStatus.ready
                + " error=\"" + p.authError + "\"")
  }

  // Renewal check: with an expired token and refresh material in the state
  // file, does the panel try the token endpoint, and what does it conclude?
  function reportRefresh(): void {
    if (!panelLoader.item) return
    var p = panelLoader.item
    console.log("[refresh] state=" + p.tokenStatus.state + " ready=" + p.tokenStatus.ready
                + " canRefresh=" + p.tokenStatus.canRefresh + " source=" + p.tokenStatus.source
                + " refreshing=" + p.refreshing + " failedAt=" + p.refreshFailedAtMs
                + " status=\"" + p.statusText + "\" error=\"" + p.authError + "\"")
  }

  function reportPersisted(): void {
    if (!panelLoader.item) return
    var t = panelLoader.item.tokenStatus
    console.log("[persist] on launch: ready=" + t.ready + " state=" + t.state
                + " expiresInDays=" + t.expiresInDays)
  }

  function reportNoToken(): void {
    if (!panelLoader.item) return
    var p = panelLoader.item
    console.log("[firstrun] tokenReady=" + p.tokenStatus.ready
                + " state=" + p.tokenStatus.state)
    p.beginSearch()
    console.log("[firstrun] after flight Search: busy=" + p.busy
                + " hasSearched=" + p.hasSearched
                + " offers=" + p.offers.length
                + " error=\"" + p.errorText + "\"")
    p.tab = "hotels"
    p.beginHotelSearch()
    console.log("[firstrun] after hotel Search: busy=" + p.hotelBusy
                + " hotelHasSearched=" + p.hotelHasSearched
                + " error=\"" + p.hotelError + "\"")
    p.tab = "flights"
  }

  function reportLinks(): void {
    if (!panelLoader.item) return
    var offers = panelLoader.item.visibleOffers
    if (!offers || offers.length === 0) { console.log("[links] no offers"); return }
    for (var i = 0; i < Math.min(3, offers.length); i++)
      console.log("[links] " + offers[i].price + " -> " + panelLoader.item.linkFor(offers[i]))
    var t = panelLoader.item.transferInfo
    console.log("[transfer] ok=" + t.ok + " price=" + t.price + " mins=" + t.minutes
                + " | mapCoord=" + JSON.stringify(panelLoader.item.mapCoord))
  }

  function grabTo(path: string): void {
    win.contentItem.grabToImage(function (result) {
      var ok = result.saveToFile(path)
      Host.grabFinished(path, ok === true)
    })
  }

  Component.onCompleted: console.log("[preview] loaded plugin from", Host.pluginDir())
}
