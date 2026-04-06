"""
Unified flight search engine — fires ALL sources in parallel:

1. Local agent-device connectors (200 airline connectors) — zero auth, free
2. Cloud Run backend API (Duffel, Amadeus, Sabre, Travelport, etc.) — paid providers

Both fire simultaneously via asyncio.gather. The backend call is a single
HTTP POST to the Cloud Run API which internally parallelizes all paid providers.
Results from both are merged, deduplicated, and sorted.

Environment variables:
  LETSFG_API_KEY  — API key for the Cloud Run backend
  LETSFG_BASE_URL — Backend URL (default: https://api.letsfg.co)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import sys
from typing import Optional

import httpx

from .combo_engine import build_combos
from .currency import fetch_rates, _fallback_convert
from .airline_routes import get_country, get_relevant_connectors, AIRLINE_COUNTRIES
from .browser import is_browser_available
from .ryanair import RyanairConnectorClient
from .wizzair import WizzairConnectorClient
from .kiwi import KiwiConnectorClient

# ── Direct airline website connectors (LCCs not in GDS) ────────────────────────
from .easyjet import EasyjetConnectorClient
from .southwest import SouthwestConnectorClient
from .airasia import AirAsiaConnectorClient
from .airasiax import AirAsiaXConnectorClient
from .indigo import IndiGoConnectorClient
from .norwegian import NorwegianConnectorClient
from .vueling import VuelingConnectorClient
from .eurowings import EurowingsConnectorClient
from .transavia import TransaviaConnectorClient
from .pegasus import PegasusConnectorClient
from .flydubai import FlydubaiConnectorClient
# Temporarily disabled due to merge conflicts:
from .spirit import SpiritConnectorClient
from .frontier import FrontierConnectorClient
from .volaris import VolarisConnectorClient
from .airarabia import AirArabiaConnectorClient
from .vietjet import VietJetConnectorClient
from .cebupacific import CebuPacificConnectorClient
from .scoot import ScootConnectorClient
from .jetsmart import JetSmartConnectorClient
from .jetstar import JetstarConnectorClient
from .jet2 import Jet2ConnectorClient
from .flynas import FlynasConnectorClient
from .gol import GolConnectorClient
from .azul import AzulConnectorClient
from .flysafair import FlySafairConnectorClient
from .vivaaerobus import VivaAerobusConnectorClient
from .allegiant import AllegiantConnectorClient
from .jetblue import JetBlueConnectorClient
from .flair import FlairConnectorClient
from .thai import ThaiConnectorClient
from .spicejet import SpiceJetConnectorClient
from .akasa import AkasaConnectorClient
from .spring import SpringConnectorClient
from .peach import PeachConnectorClient
from .zipair import ZipairConnectorClient
from .condor import CondorConnectorClient
from .sunexpress import SunExpressConnectorClient
from .volotea import VoloteaConnectorClient
from .smartwings import SmartwingsConnectorClient
from .flybondi import FlybondiConnectorClient
from .jejuair import JejuAirConnectorClient
from .twayair import TwayAirConnectorClient
from .porter import PorterConnectorClient
from .nokair import NokAirConnectorClient
from .airpeace import AirPeaceConnectorClient
from .pia import PiaConnectorClient
from .airindiaexpress import AirIndiaExpressConnectorClient
from .batikair import BatikAirConnectorClient
from .luckyair import LuckyAirConnectorClient
from .nineair import NineAirConnectorClient
from .avelo import AveloConnectorClient
from .breeze import BreezeConnectorClient
from .salamair import SalamAirConnectorClient
from .usbangla import USBanglaConnectorClient
from .biman import BimanConnectorClient
from .etihad import EtihadConnectorClient
from .turkish import TurkishConnectorClient
from .emirates import EmiratesConnectorClient
from .malaysia import MalaysiaConnectorClient
from .suncountry import SunCountryConnectorClient
from .alaska import AlaskaConnectorClient
from .hawaiian import HawaiianConnectorClient
from .american import AmericanConnectorClient
from .united import UnitedConnectorClient
from .delta import DeltaConnectorClient
from .cathay import CathayConnectorClient
from .singapore import SingaporeConnectorClient
from .korean import KoreanConnectorClient
from .nh import ANAConnectorClient
from .qantas import QantasConnectorClient
from .virginaustralia import VirginAustraliaConnectorClient
from .bangkokairways import BangkokAirwaysConnectorClient
from .aegean import AegeanConnectorClient
from .aerlingus import AerLingusConnectorClient
from .airbaltic import AirbalticConnectorClient
from .aircanada import AirCanadaConnectorClient
from .airindia import AirIndiaConnectorClient
from .airnewzealand import AirNewZealandConnectorClient
from .aerolineas import AerolineasConnectorClient
from .arajet import ArajetConnectorClient
from .chinaairlines import ChinaAirlinesConnectorClient
from .egyptair import EgyptAirConnectorClient
from .ethiopian import EthiopianConnectorClient
from .finnair import FinnairConnectorClient
from .garuda import GarudaConnectorClient
from .icelandair import IcelandairConnectorClient
from .itaairways import ITAAirwaysConnectorClient
from .jal import JapanAirlinesConnectorClient
from .jazeera import JazeeraConnectorClient
from .kenyaairways import KenyaAirwaysConnectorClient
from .flyarystan import FlyArystanConnectorClient
from .olympicair_api import OlympicAirConnectorClient
from .philippineairlines import PhilippineAirlinesConnectorClient
from .royalairmaroc import RoyalAirMarocConnectorClient
from .saa import SouthAfricanAirwaysConnectorClient
from .sas import SASConnectorClient
from .skyairline import SkyAirlineConnectorClient
from .skyexpress import SkyExpressConnectorClient
from .tap import TapConnectorClient
from .wingo import WingoConnectorClient
from .klm import KlmConnectorClient
from .airfrance import AirfranceConnectorClient
from .azerbaijanairlines import AzerbaijanairlinesConnectorClient
from .srilankan import SrilankanConnectorClient
from .iberia import IberiaConnectorClient
from .iberiaexpress import IberiaExpressConnectorClient
from .virginatlantic import VirginAtlanticConnectorClient
from .lufthansa import LufthansaConnectorClient
from .swiss import SwissConnectorClient
from .austrian import AustrianConnectorClient
from .brusselsairlines import BrusselsAirlinesConnectorClient
from .discover import DiscoverConnectorClient
from .elal import ElAlConnectorClient
from .saudia import SaudiaConnectorClient
from .omanair import OmanairConnectorClient
from .flyadeal import FlyadealConnectorClient
from .airmauritius import AirmauritiusConnectorClient
from .britishairways import BritishAirwaysConnectorClient
from .evaair import EvaAirConnectorClient
from .rex import RexConnectorClient
from .fijiairways import FijiAirwaysConnectorClient
from .airnorth import AirnorthConnectorClient
from .airchina import AirChinaConnectorClient
from .chinaeastern import ChinaEasternConnectorClient
from .chinasouthern import ChinaSouthernConnectorClient
from .vietnamairlines import VietnamAirlinesConnectorClient
from .asiana import AsianaConnectorClient
from .airtransat import AirTransatConnectorClient
from .airserbia import AirSerbiaConnectorClient
from .aireuropa import AirEuropaConnectorClient
from .mea import MEAConnectorClient
from .hainan import HainanConnectorClient
from .royaljordanian import RoyalJordanianConnectorClient
from .kuwaitairways import KuwaitAirwaysConnectorClient
from .level import LevelConnectorClient
from .qatar import QatarConnectorClient
from .aircalin import AircalinConnectorClient
# Temporarily disabled due to merge conflicts:
from .traveloka import TravelokaConnectorClient
from .wego import WegoConnectorClient
from .webjet import WebjetConnectorClient
from .tiket import TiketConnectorClient
from .tripcom import TripcomConnectorClient
from .cleartrip import CleartripConnectorClient
from .edreams import EdreamsConnectorClient
from .serpapi_google import SerpApiGoogleConnectorClient
from .despegar import DespegarConnectorClient
from .opodo import OpodoConnectorClient
from .momondo import MomondoConnectorClient
from .kayak import KayakConnectorClient
from .cheapflights import CheapflightsConnectorClient
from .skyscanner import SkyscannerConnectorClient
from .avianca import AviancaConnectorClient
from .copa import CopaConnectorClient
from .latam import LatamConnectorClient
from .lot import LotConnectorClient
from .westjet import WestjetConnectorClient
from .iwantthatflight import IWantThatFlightConnectorClient
from .airniugini import AirNiuginiConnectorClient
from .linkairways import LinkAirwaysConnectorClient
from .pngair import PNGAirConnectorClient
from .airtahitinui import AirTahitiNuiConnectorClient
from .airvanuatu import AirVanuatuConnectorClient
from .citilink import CitilinkConnectorClient
from .samoaairways import SamoaAirwaysConnectorClient
from .solomonairlines import SolomonAirlinesConnectorClient
from .superairjet import SuperAirJetConnectorClient
from .transnusa import TransNusaConnectorClient
from .caribbeanairlines import CaribbeanAirlinesConnectorClient
from .rwandair import RwandAirConnectorClient
from .airseychelles import AirSeychellesConnectorClient
from .airgreenland import AirGreenlandConnectorClient
from .starlux import StarluxConnectorClient
from .azoresairlines import AzoresAirlinesConnectorClient
from .cyprusairways import CyprusAirwaysConnectorClient
from .skiplagged import SkiplaggedConnectorClient
from .aviasales import AviasalesConnectorClient
from .travix import TravixConnectorClient
from .travelup import TravelupConnectorClient
from .lastminute import LastminuteConnectorClient
from .byojet import ByojetConnectorClient
from .yatra import YatraConnectorClient
from .etraveli import EtraveliConnectorClient, TravelgenioConnectorClient
from .ixigo import IxigoConnectorClient
from .rehlat import RehlatConnectorClient
from .travelstart import TravelstartConnectorClient
from .auntbetty import AuntbettyConnectorClient
from .flightcatchers import FlightcatchersConnectorClient
from .traveltrolley import TraveltrolleyConnectorClient
from .onthebeach import OnthebeachConnectorClient
from .agoda import AgodaConnectorClient
from .almosafer import AlmosaferConnectorClient
from .bookingcom import BookingcomConnectorClient
from .musafir import MusafirConnectorClient
from .akbartravels import AkbartravelsConnectorClient
from .airasiamove import AirasiamoveConnectorClient
from .hopper import HopperConnectorClient

from ..models.flights import AirlineSummary, FlightOffer, FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)

# Connectors that launch Chrome/Playwright browsers.
# These are throttled by a semaphore to prevent 20+ Chrome processes at once.
# In cloud/headless environments without Chrome, these are skipped entirely.
_BROWSER_SOURCES: set[str] = {
    "airasia_direct", "airasiax_direct", "allegiant_direct", "azul_direct", "batikair_direct",
    "cebupacific_direct", "condor_direct", "easyjet_direct", "eurowings_direct",
    "flybondi_direct", "flydubai_direct", "flynas_direct", "frontier_direct",
    "gol_direct", "indigo_direct", "jet2_direct", "jetsmart_direct",
    "jetstar_direct", "luckyair_direct", "9air_direct",
    "jetblue_direct", "avelo_direct", "breeze_direct",
    "norwegian_direct", "peach_direct", "pegasus_direct",
    "porter_direct", "scoot_direct", "smartwings_direct", "southwest_direct",
    "spirit_direct", "sunexpress_direct", "transavia_direct", "twayair_direct",
    "vietjet_direct", "volaris_direct", "volotea_direct", "vueling_direct",
    "usbangla_direct",
    "etihad_direct",
    "turkish_direct",
    "emirates_direct",
    "zipair_direct",
    "suncountry_direct",
    "alaska_direct",
    "hawaiian_direct",
    "american_direct",
    "united_direct",
    "delta_direct",
    # cathay → curl_cffi-only, removed from browser set
    "singapore_direct",
    "korean_direct",
    "nh_direct",
    "bangkokairways_direct",
    # aegean, sas, tap → httpx/curl_cffi-only, removed from browser set
    "airnewzealand_direct", "finnair_direct",
    "philippineairlines_direct", "qantas_direct",
    "skyairline_direct", "wingo_direct",
    "aerolineas_direct", "chinaairlines_direct",
    "saudia_direct",  # elal → httpx-only, removed
    "airchina_direct", "chinaeastern_direct", "chinasouthern_direct",
    "asiana_direct", "airtransat_direct",  # vietnamairlines → httpx-only, removed
    "airserbia_direct", "aireuropa_direct", "mea_direct",
    "hainan_direct", "royaljordanian_direct", "kuwaitairways_direct",
    "level_direct",
    "qatar_direct",
    "avianca_direct", "copa_direct", "latam_direct", "lot_direct", "westjet_direct",
    "citilink_direct", "samoaairways_direct", "superairjet_direct", "transnusa_direct",
    "traveloka_ota",
    "wego_meta",
    "webjet_ota",
    "tiket_ota",
    "edreams_ota",
    "tripcom_ota",
    "opodo_ota",
    "momondo_meta",
    "kayak_meta",
    "cheapflights_meta",
    "skyscanner_meta",

    "aviasales_meta",
    "travix_ota",
    # travelup → httpx-only, removed from browser set
    "lastminute_ota",
    "byojet_ota",
    "yatra_ota",
    "auntbetty_ota",
    "flightcatchers_ota",
    "traveltrolley_ota",
    # "onthebeach_ota",  # DEAD: package-only
    "agoda_meta",
    "almosafer_ota",
    "bookingcom_ota",
    "musafir_ota",
    "akbartravels_ota",
    "airasiamove_ota",
}


def _should_use_browsers() -> bool:
    """Decide whether browser-based connectors should run.

    Checks (in order):
    1. LETSFG_BROWSERS env var — explicit override ("0" to disable, "1" to force)
    2. LETSFG_BROWSER_WS env var — remote browser configured = browsers available
    3. Whether a Chrome/Chromium binary is available on the system
       (includes auto-discovery of Playwright's bundled Chromium)

    In cloud/agent environments, Chrome is typically available via Playwright or
    the agent platform. Xvfb is auto-started if needed (see browser.py).
    """
    override = os.environ.get("LETSFG_BROWSERS", "").strip().lower()
    if override in ("0", "false", "no", "off"):
        return False
    if override in ("1", "true", "yes", "on"):
        return True
    # Remote browser configured → all browser connectors can run through it
    if os.environ.get("LETSFG_BROWSER_WS", "").strip():
        return True
    return is_browser_available()


# Cached at module load — checked once, not per-search
_BROWSERS_AVAILABLE: bool = _should_use_browsers()

if _BROWSERS_AVAILABLE:
    _mode_detail = "remote browser (LETSFG_BROWSER_WS)" if os.environ.get("LETSFG_BROWSER_WS", "").strip() else "local Chrome/Chromium"
    logger.info("Browser mode: %s — all connectors active", _mode_detail)
else:
    logger.info("API-only mode: no browser available — %d browser connectors skipped. "
                "Using Kiwi aggregator + API-only direct connectors. "
                "Install Chrome, set LETSFG_BROWSER_WS, or LETSFG_BROWSERS=1 to enable all.",
                len(_BROWSER_SOURCES))

# Registry of direct airline connectors: (source_name, connector_class, timeout)
# All are zero-auth, always available, "free" tier.
_DIRECT_AIRLINE_connectorS: list[tuple[str, type, float]] = [
    ("easyjet_direct", EasyjetConnectorClient, 25.0),
    ("southwest_direct", SouthwestConnectorClient, 25.0),
    ("airasia_direct", AirAsiaConnectorClient, 25.0),
    ("airasiax_direct", AirAsiaXConnectorClient, 25.0),
    ("indigo_direct", IndiGoConnectorClient, 25.0),
    ("norwegian_direct", NorwegianConnectorClient, 25.0),
    ("vueling_direct", VuelingConnectorClient, 25.0),
    ("eurowings_direct", EurowingsConnectorClient, 25.0),
    ("transavia_direct", TransaviaConnectorClient, 25.0),
    ("pegasus_direct", PegasusConnectorClient, 25.0),
    ("flydubai_direct", FlydubaiConnectorClient, 25.0),
    ("spirit_direct", SpiritConnectorClient, 25.0),
    ("frontier_direct", FrontierConnectorClient, 25.0),
    ("volaris_direct", VolarisConnectorClient, 25.0),
    ("airarabia_direct", AirArabiaConnectorClient, 25.0),
    ("vietjet_direct", VietJetConnectorClient, 25.0),
    ("cebupacific_direct", CebuPacificConnectorClient, 25.0),
    ("scoot_direct", ScootConnectorClient, 25.0),
    ("jetsmart_direct", JetSmartConnectorClient, 25.0),
    ("jetstar_direct", JetstarConnectorClient, 25.0),
    ("jet2_direct", Jet2ConnectorClient, 25.0),
    ("flynas_direct", FlynasConnectorClient, 25.0),
    ("gol_direct", GolConnectorClient, 25.0),
    ("azul_direct", AzulConnectorClient, 25.0),
    ("flysafair_direct", FlySafairConnectorClient, 25.0),
    ("vivaaerobus_direct", VivaAerobusConnectorClient, 25.0),
    ("allegiant_direct", AllegiantConnectorClient, 25.0),    ("jetblue_direct", JetBlueConnectorClient, 25.0),    ("flair_direct", FlairConnectorClient, 25.0),
    ("spicejet_direct", SpiceJetConnectorClient, 25.0),
    ("akasa_direct", AkasaConnectorClient, 25.0),
    ("spring_direct", SpringConnectorClient, 25.0),
    ("peach_direct", PeachConnectorClient, 25.0),
    ("zipair_direct", ZipairConnectorClient, 25.0),
    ("condor_direct", CondorConnectorClient, 25.0),
    ("sunexpress_direct", SunExpressConnectorClient, 25.0),
    ("volotea_direct", VoloteaConnectorClient, 25.0),
    ("smartwings_direct", SmartwingsConnectorClient, 25.0),
    ("flybondi_direct", FlybondiConnectorClient, 25.0),
    ("jejuair_direct", JejuAirConnectorClient, 25.0),
    ("twayair_direct", TwayAirConnectorClient, 25.0),
    ("porter_direct", PorterConnectorClient, 25.0),
    ("nokair_direct", NokAirConnectorClient, 25.0),
    ("airpeace_direct", AirPeaceConnectorClient, 25.0),
    ("airindiaexpress_direct", AirIndiaExpressConnectorClient, 25.0),
    ("batikair_direct", BatikAirConnectorClient, 25.0),
    ("luckyair_direct", LuckyAirConnectorClient, 30.0),
    ("9air_direct", NineAirConnectorClient, 30.0),
    ("avelo_direct", AveloConnectorClient, 45.0),
    ("breeze_direct", BreezeConnectorClient, 45.0),
    ("salamair_direct", SalamAirConnectorClient, 20.0),
    ("usbangla_direct", USBanglaConnectorClient, 45.0),
    ("biman_direct", BimanConnectorClient, 25.0),
    ("etihad_direct", EtihadConnectorClient, 35.0),
    ("turkish_direct", TurkishConnectorClient, 45.0),
    ("emirates_direct", EmiratesConnectorClient, 60.0),
    ("malaysia_direct", MalaysiaConnectorClient, 25.0),
    ("suncountry_direct", SunCountryConnectorClient, 45.0),
    ("alaska_direct", AlaskaConnectorClient, 45.0),
    ("hawaiian_direct", HawaiianConnectorClient, 45.0),
    ("american_direct", AmericanConnectorClient, 45.0),
    ("united_direct", UnitedConnectorClient, 55.0),
    ("delta_direct", DeltaConnectorClient, 45.0),
    ("cathay_direct", CathayConnectorClient, 25.0),
    ("singapore_direct", SingaporeConnectorClient, 60.0),
    ("thai_direct", ThaiConnectorClient, 25.0),
    ("korean_direct", KoreanConnectorClient, 45.0),
    ("nh_direct", ANAConnectorClient, 60.0),
    ("qantas_direct", QantasConnectorClient, 25.0),
    ("virginaustralia_direct", VirginAustraliaConnectorClient, 25.0),
    ("bangkokairways_direct", BangkokAirwaysConnectorClient, 45.0),
    # ── Wired 2026-03-20: existing connectors not previously registered ──
    ("aegean_direct", AegeanConnectorClient, 45.0),
    ("aerlingus_direct", AerLingusConnectorClient, 45.0),
    ("airbaltic_direct", AirbalticConnectorClient, 15.0),
    ("aircanada_direct", AirCanadaConnectorClient, 45.0),
    ("airindia_direct", AirIndiaConnectorClient, 25.0),
    ("airnewzealand_direct", AirNewZealandConnectorClient, 45.0),
    ("arajet_direct", ArajetConnectorClient, 15.0),
    ("egyptair_direct", EgyptAirConnectorClient, 20.0),
    ("ethiopian_direct", EthiopianConnectorClient, 45.0),
    ("finnair_direct", FinnairConnectorClient, 45.0),
    ("garuda_direct", GarudaConnectorClient, 20.0),
    ("icelandair_direct", IcelandairConnectorClient, 25.0),
    ("itaairways_direct", ITAAirwaysConnectorClient, 20.0),
    ("jal_direct", JapanAirlinesConnectorClient, 20.0),
    ("jazeera_direct", JazeeraConnectorClient, 15.0),
    ("kenyaairways_direct", KenyaAirwaysConnectorClient, 45.0),
    ("olympicair_direct", OlympicAirConnectorClient, 20.0),
    ("philippineairlines_direct", PhilippineAirlinesConnectorClient, 45.0),
    ("royalairmaroc_direct", RoyalAirMarocConnectorClient, 45.0),
    ("saa_direct", SouthAfricanAirwaysConnectorClient, 45.0),
    ("sas_direct", SASConnectorClient, 25.0),
    ("skyairline_direct", SkyAirlineConnectorClient, 45.0),
    ("skyexpress_direct", SkyExpressConnectorClient, 20.0),
    ("tap_direct", TapConnectorClient, 45.0),
    ("flyarystan_direct", FlyArystanConnectorClient, 15.0),
    ("pia_direct", PiaConnectorClient, 25.0),
    ("aerolineas_direct", AerolineasConnectorClient, 45.0),
    ("chinaairlines_direct", ChinaAirlinesConnectorClient, 35.0),
    ("wingo_direct", WingoConnectorClient, 45.0),
    ("klm_direct", KlmConnectorClient, 25.0),
    ("airfrance_direct", AirfranceConnectorClient, 25.0),
    ("iberia_direct", IberiaConnectorClient, 25.0),
    ("iberiaexpress_direct", IberiaExpressConnectorClient, 25.0),
    ("virginatlantic_direct", VirginAtlanticConnectorClient, 25.0),
    # ── Lufthansa Group (curl_cffi JSON-LD extraction) ──
    ("lufthansa_direct", LufthansaConnectorClient, 20.0),
    ("swiss_direct", SwissConnectorClient, 20.0),
    ("austrian_direct", AustrianConnectorClient, 20.0),
    ("brusselsairlines_direct", BrusselsAirlinesConnectorClient, 20.0),
    ("discover_direct", DiscoverConnectorClient, 20.0),
    # ── Middle East Playwright connectors (CDP Chrome + form fill) ──
    ("elal_direct", ElAlConnectorClient, 55.0),
    ("saudia_direct", SaudiaConnectorClient, 55.0),
    # ── Oman Air (EveryMundo sputnik API — no browser) ──
    ("omanair_direct", OmanairConnectorClient, 25.0),
    # ── Azerbaijan Airlines (EveryMundo sputnik API — no browser) ──
    ("azerbaijanairlines_direct", AzerbaijanairlinesConnectorClient, 25.0),
    # ── SriLankan Airlines (EveryMundo sputnik API — no browser) ──
    ("srilankan_direct", SrilankanConnectorClient, 25.0),
    # ── flyadeal (EveryMundo sputnik API — no browser) ──
    ("flyadeal_direct", FlyadealConnectorClient, 25.0),
    # ── Air Mauritius (EveryMundo sputnik API — no browser) ──
    ("airmauritius_direct", AirmauritiusConnectorClient, 25.0),
    # ── British Airways (SOLR pricing feed via curl_cffi) ──
    ("britishairways_direct", BritishAirwaysConnectorClient, 25.0),
    # ── EVA Air (EveryMundo __NEXT_DATA__ via curl_cffi) ──
    ("evaair_direct", EvaAirConnectorClient, 25.0),
    # ── Rex Airlines (EveryMundo __NEXT_DATA__ via curl_cffi) ──
    ("rex_direct", RexConnectorClient, 25.0),
    # ── Fiji Airways (EveryMundo __NEXT_DATA__ via curl_cffi) ──
    ("fijiairways_direct", FijiAirwaysConnectorClient, 25.0),
    # ── Airnorth (.NET B2C form POST via curl_cffi) ──
    ("airnorth_direct", AirnorthConnectorClient, 30.0),
    # ── I Want That Flight (AU fare aggregator — HTML scraping) ──
    ("iwantthatflight_direct", IWantThatFlightConnectorClient, 20.0),
    # ── Air Niugini (EveryMundo __NEXT_DATA__ via curl_cffi) ──
    ("airniugini_direct", AirNiuginiConnectorClient, 25.0),
    # ── Link Airways (Playwright ASP.NET WebForms) ──
    ("linkairways_direct", LinkAirwaysConnectorClient, 35.0),
    # ── PNG Air (VARS PSS AJAX via curl_cffi) ──
    ("pngair_direct", PNGAirConnectorClient, 35.0),
    # ── CDP Chrome browser connectors (Batch 5/6/7 — form fill + API intercept) ──
    ("airchina_direct", AirChinaConnectorClient, 55.0),
    ("chinaeastern_direct", ChinaEasternConnectorClient, 55.0),
    ("chinasouthern_direct", ChinaSouthernConnectorClient, 55.0),
    ("vietnamairlines_direct", VietnamAirlinesConnectorClient, 55.0),
    ("asiana_direct", AsianaConnectorClient, 55.0),
    ("airtransat_direct", AirTransatConnectorClient, 55.0),
    ("airserbia_direct", AirSerbiaConnectorClient, 55.0),
    ("aireuropa_direct", AirEuropaConnectorClient, 55.0),
    ("mea_direct", MEAConnectorClient, 55.0),
    ("hainan_direct", HainanConnectorClient, 55.0),
    ("royaljordanian_direct", RoyalJordanianConnectorClient, 55.0),
    ("kuwaitairways_direct", KuwaitAirwaysConnectorClient, 55.0),
    ("level_direct", LevelConnectorClient, 55.0),
    ("qatar_direct", QatarConnectorClient, 55.0),
    ("aircalin_direct", AircalinConnectorClient, 25.0),
    ("traveloka_ota", TravelokaConnectorClient, 55.0),
    ("wego_meta", WegoConnectorClient, 55.0),
    ("webjet_ota", WebjetConnectorClient, 55.0),
    ("tiket_ota", TiketConnectorClient, 55.0),
    ("tripcom_ota", TripcomConnectorClient, 55.0),
    ("cleartrip_ota", CleartripConnectorClient, 55.0),
    ("edreams_ota", EdreamsConnectorClient, 55.0),
    ("despegar_ota", DespegarConnectorClient, 55.0),
    ("opodo_ota", OpodoConnectorClient, 55.0),
    ("momondo_meta", MomondoConnectorClient, 55.0),
    ("kayak_meta", KayakConnectorClient, 55.0),
    ("cheapflights_meta", CheapflightsConnectorClient, 55.0),
    ("skyscanner_meta", SkyscannerConnectorClient, 55.0),
    ("serpapi_google", SerpApiGoogleConnectorClient, 30.0),
    ("avianca_direct", AviancaConnectorClient, 45.0),
    ("copa_direct", CopaConnectorClient, 45.0),
    ("latam_direct", LatamConnectorClient, 50.0),
    ("lot_direct", LotConnectorClient, 55.0),
    ("westjet_direct", WestjetConnectorClient, 45.0),
    ("airtahitinui_direct", AirTahitiNuiConnectorClient, 25.0),
    ("airvanuatu_direct", AirVanuatuConnectorClient, 25.0),
    ("citilink_direct", CitilinkConnectorClient, 45.0),
    ("samoaairways_direct", SamoaAirwaysConnectorClient, 45.0),
    ("solomonairlines_direct", SolomonAirlinesConnectorClient, 25.0),
    ("superairjet_direct", SuperAirJetConnectorClient, 45.0),
    ("transnusa_direct", TransNusaConnectorClient, 45.0),
    # ── Caribbean Airlines (EveryMundo __NEXT_DATA__ via curl_cffi) ──
    ("caribbeanairlines_direct", CaribbeanAirlinesConnectorClient, 25.0),
    # ── RwandAir (EveryMundo __NEXT_DATA__ via curl_cffi) ──
    ("rwandair_direct", RwandAirConnectorClient, 25.0),
    # ── Air Seychelles (EveryMundo __NEXT_DATA__ via curl_cffi) ──
    ("airseychelles_direct", AirSeychellesConnectorClient, 25.0),
    # ── Air Greenland (EveryMundo __NEXT_DATA__ via curl_cffi) ──
    ("airgreenland_direct", AirGreenlandConnectorClient, 25.0),
    # ── Starlux (EveryMundo __NEXT_DATA__ via curl_cffi) ──
    ("starlux_direct", StarluxConnectorClient, 25.0),
    # ── Azores Airlines / SATA (EveryMundo __NEXT_DATA__ via curl_cffi) ──
    ("azoresairlines_direct", AzoresAirlinesConnectorClient, 25.0),
    # ── Cyprus Airways (EveryMundo __NEXT_DATA__ via curl_cffi) ──
    ("cyprusairways_direct", CyprusAirwaysConnectorClient, 25.0),
    # ── New OTA/meta connectors (Instance B batch) ──
    ("skiplagged_meta", SkiplaggedConnectorClient, 25.0),
    # ── New OTA/meta connectors (Instance A batch — Playwright + API interception) ──
    ("etraveli_ota", EtraveliConnectorClient, 55.0),
    # DEAD: Travelgenio backend APIs all return 404 (site decommissioned as of 2026-03)
    # ("travelgenio_ota", TravelgenioConnectorClient, 55.0),
    ("ixigo_meta", IxigoConnectorClient, 55.0),
    ("rehlat_ota", RehlatConnectorClient, 55.0),
    ("travelstart_ota", TravelstartConnectorClient, 45.0),
    # ── Rebuilt CDP Chrome connectors (Instance B batch) ──
    ("aviasales_meta", AviasalesConnectorClient, 55.0),
    ("travix_ota", TravixConnectorClient, 55.0),
    ("travelup_ota", TravelupConnectorClient, 55.0),
    ("lastminute_ota", LastminuteConnectorClient, 55.0),
    ("byojet_ota", ByojetConnectorClient, 55.0),
    ("yatra_ota", YatraConnectorClient, 55.0),
    # ── OTA expansion batch (Instance A — CDP Chrome / Playwright) ──
    ("auntbetty_ota", AuntbettyConnectorClient, 55.0),
    ("flightcatchers_ota", FlightcatchersConnectorClient, 55.0),
    ("traveltrolley_ota", TraveltrolleyConnectorClient, 60.0),
    # DEAD: OnTheBeach is package-holiday only, no flight-only search supported
    # ("onthebeach_ota", OnthebeachConnectorClient, 60.0),
    ("agoda_meta", AgodaConnectorClient, 55.0),
    ("almosafer_ota", AlmosaferConnectorClient, 60.0),
    ("bookingcom_ota", BookingcomConnectorClient, 65.0),
    # ── OTA expansion batch (Instance B — Playwright + API interception) ──
    ("musafir_ota", MusafirConnectorClient, 55.0),
    ("akbartravels_ota", AkbartravelsConnectorClient, 55.0),
    ("airasiamove_ota", AirasiamoveConnectorClient, 55.0),
    # ── Hopper (direct commerce API — no browser needed) ──
    ("hopper_direct", HopperConnectorClient, 25.0),
]


def _extract_legs_from_roundtrip(
    offers: list[FlightOffer],
    outbound_legs: list[FlightOffer],
    return_legs: list[FlightOffer],
) -> None:
    """Decompose round-trip offers into one-way legs for the combo engine.

    For airlines like Wizzair/Ryanair that price legs independently,
    we extract each direction so the combo engine can mix airlines.
    """
    seen_out: set[str] = set()
    seen_ret: set[str] = set()

    for offer in offers:
        if not offer.outbound or not offer.inbound:
            continue

        ob_key = "|".join(
            f"{s.flight_no}_{s.departure.isoformat()}"
            for s in offer.outbound.segments
        )
        rt_key = "|".join(
            f"{s.flight_no}_{s.departure.isoformat()}"
            for s in offer.inbound.segments
        )

        # Estimate per-leg price as half (the combo engine prices from
        # the cheapest one-way leg it finds across all sources)
        half_price = round(offer.price / 2, 2)

        if ob_key not in seen_out:
            seen_out.add(ob_key)
            outbound_legs.append(FlightOffer(
                id=f"{offer.id}_ob",
                price=half_price,
                currency=offer.currency,
                price_formatted=f"{half_price:.2f} {offer.currency}",
                price_normalized=offer.price_normalized / 2 if offer.price_normalized else None,
                outbound=offer.outbound,
                inbound=None,
                airlines=offer.airlines,
                owner_airline=offer.owner_airline,
                booking_url=offer.booking_url,
                is_locked=False,
                source=offer.source,
                source_tier=offer.source_tier,
            ))

        if rt_key not in seen_ret:
            seen_ret.add(rt_key)
            return_legs.append(FlightOffer(
                id=f"{offer.id}_rt",
                price=half_price,
                currency=offer.currency,
                price_formatted=f"{half_price:.2f} {offer.currency}",
                price_normalized=offer.price_normalized / 2 if offer.price_normalized else None,
                outbound=offer.inbound,  # inbound route becomes "outbound" of return leg
                inbound=None,
                airlines=offer.airlines,
                owner_airline=offer.owner_airline,
                booking_url=offer.booking_url,
                is_locked=False,
                source=offer.source,
                source_tier=offer.source_tier,
            ))


class MultiProvider:
    """Searches ALL flight sources in parallel — local connectors + Cloud Run backend."""

    _BACKEND_URL = (os.environ.get("LETSFG_BASE_URL") or os.environ.get("BOOSTEDTRAVEL_BASE_URL") or "https://api.letsfg.co").rstrip("/")
    _BACKEND_KEY = os.environ.get("LETSFG_API_KEY") or os.environ.get("BOOSTEDTRAVEL_API_KEY", "")
    _BACKEND_TIMEOUT = 30.0  # Backend queries paid APIs in parallel; 30s covers slowest GDS

    # ── Backend availability ─────────────────────────────────────────────

    @property
    def backend_available(self) -> bool:
        return bool(self._BACKEND_KEY)

    # Direct airline connectors — always available, no API keys needed
    @property
    def ryanair_connector_available(self) -> bool:
        return True

    @property
    def wizzair_connector_available(self) -> bool:
        return True

    @property
    def kiwi_connector_available(self) -> bool:
        return True

    def _get_ryanair_connector(self) -> Optional[RyanairConnectorClient]:
        return RyanairConnectorClient(timeout=20.0)

    def _get_wizzair_connector(self) -> Optional[WizzairConnectorClient]:
        return WizzairConnectorClient(timeout=25.0)

    def _get_kiwi_connector(self) -> Optional[KiwiConnectorClient]:
        return KiwiConnectorClient(timeout=25.0)

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search flights across ALL sources in parallel:
        - 1 async HTTP call to Cloud Run backend (paid APIs: Duffel, Amadeus, Sabre, etc.)
        - 46+ local connector tasks (LCC scrapers running on agent device)
        - Ryanair, Wizzair, Kiwi connectors (special handling)
        - Combo engine for cross-airline virtual interlining

        Everything fires at once via asyncio.gather. Total wall-clock time =
        max(backend_latency, slowest_local_connector) — typically 5-15s.

        All browser instances launched by connectors are automatically
        closed after results are collected.
        """
        try:
            return await self._search_flights_inner(req)
        finally:
            await self._cleanup_connectors()

    async def _search_flights_inner(self, req: FlightSearchRequest) -> FlightSearchResponse:
        tasks = []
        providers_used = []

        # ── Log proxy status ──
        from .browser import proxy_is_configured, get_default_proxy_url
        if proxy_is_configured():
            # Mask credentials in log
            raw = get_default_proxy_url()
            from urllib.parse import urlparse
            p = urlparse(raw)
            masked = f"{p.scheme}://{p.hostname}:{p.port}"
            logger.info("LETSFG_PROXY active: %s — all connectors routing through proxy", masked)
        else:
            logger.debug("No LETSFG_PROXY set — connectors using direct connections")

        # ── Cloud Run backend (paid API providers: Duffel, Amadeus, Sabre, etc.) ──
        if self.backend_available:
            tasks.append(self._search_backend(req))
            providers_used.append("backend")
        else:
            logger.warning("No LETSFG_API_KEY — skipping Cloud Run backend (paid APIs)")

        # ── Local connectors: Ryanair, Wizzair, Kiwi (special handling) ──
        origin_country = get_country(req.origin)
        dest_country = get_country(req.destination)

        ryanair_connector = self._get_ryanair_connector()
        wizzair_connector = self._get_wizzair_connector()
        kiwi_connector = self._get_kiwi_connector()

        ryanair_countries = AIRLINE_COUNTRIES.get("ryanair")
        if ryanair_connector and (not origin_country or not dest_country or not ryanair_countries
                or (origin_country in ryanair_countries and dest_country in ryanair_countries)):
            tasks.append(self._search_ryanair_direct(ryanair_connector, req))
            providers_used.append("ryanair_direct")

        # Wizzair requires Chrome (CDP) — skip when browsers unavailable
        wizz_countries = AIRLINE_COUNTRIES.get("wizz")
        if _BROWSERS_AVAILABLE and wizzair_connector and (
                not origin_country or not dest_country or not wizz_countries
                or (origin_country in wizz_countries and dest_country in wizz_countries)):
            tasks.append(self._search_wizzair_direct(wizzair_connector, req))
            providers_used.append("wizzair_direct")

        # Kiwi is a global aggregator — always query it
        if kiwi_connector:
            tasks.append(self._search_kiwi_connector(kiwi_connector, req))
            providers_used.append("kiwi_connector")

        # ── Direct airline website connectors (46 LCCs) — route-filtered ──
        filtered_connectors = get_relevant_connectors(req.origin, req.destination, _DIRECT_AIRLINE_connectorS)

        # Skip browser-based connectors when Chrome is not available
        # (cloud/agent environments). API-only connectors still run.
        browser_skipped = 0
        if not _BROWSERS_AVAILABLE:
            before = len(filtered_connectors)
            filtered_connectors = [
                (src, cls, t) for src, cls, t in filtered_connectors
                if src not in _BROWSER_SOURCES
            ]
            browser_skipped = before - len(filtered_connectors)
            if browser_skipped:
                logger.info("No browser available — skipped %d browser-based connectors "
                            "(API-only connectors + Kiwi aggregator still active)",
                            browser_skipped)

        skipped = len(_DIRECT_AIRLINE_connectorS) - len(filtered_connectors)
        if skipped - browser_skipped > 0:
            logger.info("Route filter: %s->%s -- skipped %d/%d irrelevant connectors",
                        req.origin, req.destination, skipped - browser_skipped,
                        len(_DIRECT_AIRLINE_connectorS))

        # Smart ordering: API-only connectors first (instant, no Chrome needed),
        # then browser connectors sorted by timeout ascending (fast scrapers get
        # semaphore slots before slow ones). On weak VMs this means results start
        # flowing while heavy browser connectors queue behind the semaphore.
        api_connectors = [(s, c, t) for s, c, t in filtered_connectors if s not in _BROWSER_SOURCES]
        browser_connectors = sorted(
            ((s, c, t) for s, c, t in filtered_connectors if s in _BROWSER_SOURCES),
            key=lambda x: x[2],  # sort by timeout (fastest first)
        )
        filtered_connectors = api_connectors + browser_connectors

        for source, connector_cls, timeout in filtered_connectors:
            connector = connector_cls(timeout=timeout)
            # Resolve city codes to primary airport for connectors that need it
            connector_req = req
            if source not in self._CITY_CODE_AWARE:
                resolved_origin = self._resolve_primary(req.origin)
                resolved_dest = self._resolve_primary(req.destination)
                if resolved_origin != req.origin or resolved_dest != req.destination:
                    connector_req = req.model_copy(update={
                        "origin": resolved_origin,
                        "destination": resolved_dest,
                    })
            tasks.append(self._search_connector_generic(connector, connector_req, source))
            providers_used.append(source)

        # ── Combo engine: one-way legs for cross-airline virtual interlining ──
        combo_tasks: list[asyncio.Task] = []
        combo_labels: list[str] = []
        is_round_trip = bool(req.return_from)

        if is_round_trip:
            # Build one-way requests for each direction
            outbound_req = req.model_copy(update={
                "return_from": None, "return_to": None,
            })
            return_req = req.model_copy(update={
                "origin": req.destination,
                "destination": req.origin,
                "date_from": req.return_from,
                "date_to": req.return_to,
                "return_from": None,
                "return_to": None,
            })

            # Fire one-way searches through direct connectors for combo engine.
            # SKIP Wizzair — its round-trip search already returns separate outbound/return
            # flights.  We extract those as one-way legs below to avoid extra API calls
            # that trigger rate limiting.
            for label, getter in [
                ("ryanair_direct", self._get_ryanair_connector),
                ("kiwi_connector", self._get_kiwi_connector),
            ]:
                client_out = getter()
                client_ret = getter()
                if client_out:
                    search_fn = self._combo_search_fn(label)
                    combo_tasks.append(search_fn(client_out, outbound_req))
                    combo_labels.append(f"{label}_out")
                    combo_tasks.append(search_fn(client_ret, return_req))
                    combo_labels.append(f"{label}_ret")

            # ── Round-trip return leg search (ALL connectors) ──
            # Most airline connectors ignore return_date and only return outbound.
            # Fire a reverse one-way search so the combo engine can build proper
            # round-trip offers (e.g. EasyJet outbound + Norwegian return, or
            # same-airline combos like Spirit out + Spirit back).
            # All connectors — including browser-based — are fired for the
            # return direction.  The browser semaphore limits Chrome concurrency;
            # extra tasks simply queue until a slot is free.
            return_filtered = get_relevant_connectors(
                req.destination, req.origin, _DIRECT_AIRLINE_connectorS
            )
            # Skip browser connectors when Chrome is not available (same as outbound)
            if not _BROWSERS_AVAILABLE:
                return_filtered = [
                    (s, c, t) for s, c, t in return_filtered
                    if s not in _BROWSER_SOURCES
                ]
            # Smart ordering: API connectors first (instant), then browser
            # connectors sorted by timeout ascending (fast scrapers get
            # semaphore slots before slow ones).
            api_ret = [(s, c, t) for s, c, t in return_filtered if s not in _BROWSER_SOURCES]
            browser_ret = sorted(
                ((s, c, t) for s, c, t in return_filtered if s in _BROWSER_SOURCES),
                key=lambda x: x[2],
            )
            return_filtered = api_ret + browser_ret
            for source, connector_cls, timeout in return_filtered:
                connector = connector_cls(timeout=timeout)
                # Resolve city codes for non-city-code-aware connectors
                combo_req = return_req
                if source not in self._CITY_CODE_AWARE:
                    r_origin = self._resolve_primary(return_req.origin)
                    r_dest = self._resolve_primary(return_req.destination)
                    if r_origin != return_req.origin or r_dest != return_req.destination:
                        combo_req = return_req.model_copy(update={
                            "origin": r_origin, "destination": r_dest,
                        })
                combo_tasks.append(
                    self._search_connector_generic(connector, combo_req, source)
                )
                combo_labels.append(f"{source}_ret")

        if not tasks:
            logger.error("No flight providers configured!")
            return FlightSearchResponse(
                search_id="",
                origin=req.origin,
                destination=req.destination,
                currency=req.currency,
                offers=[],
                total_results=0,
            )

        # Run all providers in parallel (normal + combo one-way searches)
        all_tasks = tasks + combo_tasks
        results = await asyncio.gather(*all_tasks, return_exceptions=True)

        # Split results: normal providers vs combo legs
        normal_results = results[:len(tasks)]
        combo_results = results[len(tasks):]

        # Collect offers from successful providers
        all_offers: list[FlightOffer] = []
        merged_passenger_ids = []
        merged_offer_request_id = ""

        for i, result in enumerate(normal_results):
            provider = providers_used[i]
            if isinstance(result, Exception):
                logger.error("Provider %s failed: %s", provider, result)
                continue
            if isinstance(result, FlightSearchResponse):
                all_offers.extend(result.offers)
                # Keep backend's passenger_ids and offer_request_id (needed for bookings)
                if provider == "backend" and result.passenger_ids:
                    merged_passenger_ids = result.passenger_ids
                    merged_offer_request_id = result.offer_request_id

        # ── Fire-and-forget telemetry: report connector outcomes to backend ──
        self._send_telemetry(
            providers_used, normal_results, req,
        )

        # ── Build cross-airline combos from one-way legs ──
        if is_round_trip and (combo_results or True):
            outbound_legs: list[FlightOffer] = []
            return_legs: list[FlightOffer] = []

            # ── Harvest outbound legs from normal provider results ──
            # Direct airline connectors only return one-way outbound offers
            # even for round-trip requests.  Re-use those as outbound combo
            # legs so the combo engine can pair them with return legs from
            # other airlines (e.g. VS outbound + AI return).
            _SKIP_FOR_COMBO = {"backend", "kiwi_connector", "ryanair_direct", "wizzair_direct"}
            for i, result in enumerate(normal_results):
                provider = providers_used[i]
                if provider in _SKIP_FOR_COMBO:
                    continue  # already in combo pipeline or handled separately
                if isinstance(result, FlightSearchResponse):
                    for offer in result.offers:
                        if offer.outbound and not offer.inbound:
                            outbound_legs.append(offer)

            for i, result in enumerate(combo_results):
                if isinstance(result, Exception):
                    logger.debug("Combo leg %s failed: %s", combo_labels[i], result)
                    continue
                if isinstance(result, FlightSearchResponse):
                    direction = combo_labels[i]
                    for offer in result.offers:
                        if direction.endswith("_out"):
                            outbound_legs.append(offer)
                        else:
                            return_legs.append(offer)

            # Extract one-way legs from round-trip results (Wizzair & Kiwi).
            # Avoids extra API calls — their RT offers already contain both
            # outbound + inbound legs that the combo engine can mix with
            # legs from other airlines.
            for rt_provider in ("wizzair_direct", "kiwi_connector"):
                rt_idx = None
                for i, p in enumerate(providers_used):
                    if p == rt_provider:
                        rt_idx = i
                        break
                if rt_idx is not None:
                    rt_result = normal_results[rt_idx]
                    if isinstance(rt_result, FlightSearchResponse):
                        _extract_legs_from_roundtrip(rt_result.offers, outbound_legs, return_legs)

            # Normalize one-way leg prices before combining
            await self._normalize_prices(outbound_legs, req.currency)
            await self._normalize_prices(return_legs, req.currency)

            # Filter legs to correct dates — some connectors (e.g. OmanAir sputnik)
            # return fares across a wide date range. Only keep legs matching the
            # requested outbound/return dates (±1 day tolerance).
            outbound_legs = self._filter_legs_by_date(outbound_legs, req.date_from)
            if req.return_from:
                return_legs = self._filter_legs_by_date(return_legs, req.return_from)

            # Build cross-airlines combos
            combos = build_combos(outbound_legs, return_legs, req.currency)
            logger.info(
                "Combo engine produced %d cross-airline offers from %d out + %d ret legs",
                len(combos), len(outbound_legs), len(return_legs),
            )
            all_offers.extend(combos)

        # Normalize prices to requested currency for fair comparison
        await self._normalize_prices(all_offers, req.currency)

        # Deduplicate similar offers (same route, similar time, similar price)
        deduped = self._deduplicate(all_offers)

        # ── Filter by max_stopovers ────────────────────────────────────────
        # Applied post-aggregate so ALL sources (local + backend) respect it.
        if req.max_stopovers is not None:
            before_count = len(deduped)
            deduped = [
                o for o in deduped
                if (o.outbound is None or o.outbound.stopovers <= req.max_stopovers)
                and (o.inbound is None or o.inbound.stopovers <= req.max_stopovers)
            ]
            filtered_count = before_count - len(deduped)
            if filtered_count:
                logger.info("max_stopovers=%d filter removed %d offers (%d remain)",
                            req.max_stopovers, filtered_count, len(deduped))

        # ── Route validation ───────────────────────────────────────────────
        # Reject offers where outbound origin/destination don't match the
        # requested route.  Catches connectors that return wrong routes
        # (e.g. Singapore connector returning SIN→LHR for a LON→DEL search).
        deduped = self._filter_wrong_routes(deduped, req)

        # ── Round-trip preference ──────────────────────────────────────────
        # When a round-trip was requested, prefer offers that include both
        # outbound and inbound routes.  One-way offers (inbound is None)
        # are dropped when true round-trip offers are available; kept only
        # as a fallback when no connector returned a proper RT result.
        if is_round_trip:
            rt_offers = [o for o in deduped if o.inbound is not None]
            if rt_offers:
                ow_dropped = len(deduped) - len(rt_offers)
                if ow_dropped:
                    logger.info(
                        "RT preference: keeping %d round-trip offers, "
                        "dropping %d one-way",
                        len(rt_offers), ow_dropped,
                    )
                deduped = rt_offers

        # --- Airline-diverse selection ---
        # Ensure at least the cheapest offer per airline is included,
        # then fill remaining slots with overall cheapest.
        deduped = self._diverse_select(deduped, req.sort, req.limit)

        # Build airlines summary (cheapest per airline across ALL deduped offers)
        airlines_summary = self._build_airlines_summary(all_offers)

        search_hash = hashlib.md5(
            f"{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        # Build source tiers map from providers that returned results
        source_tiers: dict[str, str] = {}
        tier_providers: dict[str, list[str]] = {}
        for offer in deduped:
            tier = offer.source_tier or "paid"
            src = offer.source or "unknown"
            if tier not in tier_providers:
                tier_providers[tier] = []
            if src not in tier_providers[tier]:
                tier_providers[tier].append(src)
        for tier, srcs in tier_providers.items():
            source_tiers[tier] = ", ".join(srcs)

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            offer_request_id=merged_offer_request_id,
            passenger_ids=merged_passenger_ids,
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=deduped,
            total_results=len(deduped),
            airlines_summary=airlines_summary,
            search_params={},
            source_tiers=source_tiers,
        )

    # ── Telemetry: report connector health to backend ────────────────────────

    def _send_telemetry(
        self,
        providers_used: list[str],
        results: list,
        req: FlightSearchRequest,
    ) -> None:
        """Fire-and-forget POST to report which connectors worked/failed.

        Runs in a background task so it never blocks the search response.
        Only reports local connector results (skips 'backend').
        Silently swallows all errors — telemetry must never break search.
        """
        # Collect per-connector outcomes
        connector_results = []
        for i, result in enumerate(results):
            provider = providers_used[i]
            if provider == "backend":
                continue  # backend tracks its own connectors server-side
            if isinstance(result, Exception):
                connector_results.append({
                    "connector": provider, "ok": False, "offers": 0, "latency_ms": 0,
                })
            elif isinstance(result, FlightSearchResponse):
                # ok=True if connector ran successfully (even with 0 offers)
                # 0 offers is valid when route doesn't exist on that airline
                connector_results.append({
                    "connector": provider,
                    "ok": True,
                    "offers": result.total_results,
                    "latency_ms": 0,
                })

        if not connector_results:
            return

        route = f"{req.origin}-{req.destination}"
        asyncio.ensure_future(self._post_telemetry(connector_results, route))

    async def _post_telemetry(
        self, connector_results: list[dict], route: str,
    ) -> None:
        """Background POST of connector telemetry to backend API."""
        try:
            from letsfg import __version__
        except Exception:
            __version__ = "unknown"

        payload = {
            "route": route,
            "sdk_version": __version__,
            "client_type": "local-engine",
            "results": connector_results,
        }

        url = f"{self._BACKEND_URL}/api/v1/analytics/telemetry/connector-results"
        headers = {"Content-Type": "application/json"}
        if self._BACKEND_KEY:
            headers["X-API-Key"] = self._BACKEND_KEY

        try:
            async with httpx.AsyncClient(timeout=5) as client:
                await client.post(url, json=payload, headers=headers)
        except Exception:
            pass  # telemetry must never break search

    # ── Per-connector browser cleanup ──────────────────────────────────────────

    async def _cleanup_single_connector(self, client) -> None:
        """Close a single connector's module-level browser resources immediately.

        Called after a browser-based connector finishes so its Chrome process
        is freed without waiting for the full search to complete.
        """
        from connectors.browser import cleanup_module_browsers

        mod = sys.modules.get(type(client).__module__)
        if mod:
            closed = await cleanup_module_browsers(mod)
            if closed:
                logger.debug("Early cleanup: closed %d resource(s) for %s",
                             closed, type(client).__name__)

    # ── Full browser cleanup ─────────────────────────────────────────────────────

    async def _cleanup_connectors(self):
        """Close all browser instances launched by connectors during search.

        Introspects module-level globals (_browser, _chrome_proc, _pw_instance,
        _nd_browser etc.) in every imported connector module and terminates them.
        Only affects processes we created — never kills the user's own Chrome.
        """
        from connectors.browser import cleanup_module_browsers, cleanup_all_browsers

        modules_to_clean = []
        seen = set()

        # All direct airline connector modules
        for _source, cls, _timeout in _DIRECT_AIRLINE_connectorS:
            mod = sys.modules.get(cls.__module__)
            if mod and id(mod) not in seen:
                seen.add(id(mod))
                modules_to_clean.append(mod)

        # Ryanair, Wizzair, Kiwi connector modules
        for mod_name in ('connectors.ryanair', 'connectors.wizzair', 'connectors.kiwi_connector'):
            mod = sys.modules.get(mod_name)
            if mod and id(mod) not in seen:
                seen.add(id(mod))
                modules_to_clean.append(mod)

        per_mod = await cleanup_module_browsers(*modules_to_clean)
        helper = await cleanup_all_browsers()
        total = per_mod + helper
        if total:
            logger.info("Browser cleanup: closed %d resource(s) across %d modules",
                        total, len(modules_to_clean))

    # ── Cloud Run backend (single HTTP call, fires all paid APIs in parallel on server) ──

    async def _search_backend(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Call the Cloud Run backend API which queries all paid providers
        (Duffel, Amadeus, Sabre, Travelport, Kiwi, Travelpayouts, MCP, Trip.com)
        in parallel on the server side. Returns merged results.
        """
        body = {
            "origin": req.origin,
            "destination": req.destination,
            "date_from": str(req.date_from),
            "adults": req.adults,
            "children": req.children,
            "infants": req.infants,
            "max_stopovers": req.max_stopovers,
            "currency": req.currency,
            "limit": req.limit,
            "sort": req.sort,
        }
        if req.date_to:
            body["date_to"] = str(req.date_to)
        if req.return_from:
            body["return_from"] = str(req.return_from)
        if req.return_to:
            body["return_to"] = str(req.return_to)
        if req.cabin_class:
            body["cabin_class"] = req.cabin_class

        url = f"{self._BACKEND_URL}/api/v1/flights/search"
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self._BACKEND_KEY,
            "User-Agent": "letsfg-engine/1.0",
        }

        async with httpx.AsyncClient(timeout=self._BACKEND_TIMEOUT) as client:
            resp = await client.post(url, json=body, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        result = FlightSearchResponse(**data)
        logger.info("Backend returned %d offers from %s",
                     len(result.offers), result.source_tiers)
        return result

    # ── Local connector search methods ───────────────────────────────────

    async def _search_ryanair_direct(
        self, client: RyanairConnectorClient, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        """Search Ryanair's website API directly — definitive LCC pricing."""
        try:
            result = await client.search_flights(req)
            for offer in result.offers:
                offer.source = "ryanair_direct"
                offer.source_tier = "free"
            return result
        finally:
            await client.close()

    async def _search_wizzair_direct(
        self, client: WizzairConnectorClient, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        """Search Wizzair's website API directly — definitive LCC pricing."""
        from connectors.browser import acquire_browser_slot, release_browser_slot
        await acquire_browser_slot()
        try:
            result = await asyncio.wait_for(
                client.search_flights(req), timeout=90,
            )
            for offer in result.offers:
                offer.source = "wizzair_direct"
                offer.source_tier = "free"
            return result
        except asyncio.TimeoutError:
            logger.warning("wizzair_direct timed out after 90s")
            return FlightSearchResponse(
                search_id="", origin=req.origin, destination=req.destination,
                currency=req.currency, offers=[], total_results=0,
            )
        except BaseException as exc:
            logger.warning("wizzair_direct crashed: %s", type(exc).__name__)
            return FlightSearchResponse(
                search_id="", origin=req.origin, destination=req.destination,
                currency=req.currency, offers=[], total_results=0,
            )
        finally:
            try:
                await client.close()
            except Exception:
                pass
            release_browser_slot()

    async def _search_kiwi_connector(
        self, client: KiwiConnectorClient, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        """Search Kiwi.com's public Skypicker API — LCCs + virtual interlining."""
        try:
            result = await client.search_flights(req)
            for offer in result.offers:
                offer.source = "kiwi_connector"
                offer.source_tier = "free"
            return result
        finally:
            await client.close()

    async def _search_connector_generic(
        self, client, req: FlightSearchRequest, source: str
    ) -> FlightSearchResponse:
        """Generic wrapper for direct airline connectors — tags source/tier, ensures cleanup.

        Browser-based connectors are throttled by a semaphore so at most N
        Chrome processes run simultaneously (prevents resource exhaustion).

        Slot acquisition has a separate generous timeout (5 min) so connectors
        aren't starved, then the search itself gets its own hard timeout.

        Catches ALL exceptions (including CancelledError) so no single
        connector can crash the entire search.
        """
        uses_browser = source in _BROWSER_SOURCES
        _empty = FlightSearchResponse(
            search_id="", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
        _search_timeout = 90 if uses_browser else 45
        _slot_timeout = 300  # 5 min max to wait for a browser slot
        slot_acquired = False
        try:
            # Phase 1: acquire browser slot (generous timeout, separate from search)
            if uses_browser:
                from connectors.browser import acquire_browser_slot
                logger.warning("%s waiting for browser slot…", source)
                try:
                    await asyncio.wait_for(acquire_browser_slot(), timeout=_slot_timeout)
                except asyncio.TimeoutError:
                    logger.warning("%s gave up waiting for browser slot after %ds", source, _slot_timeout)
                    return _empty
                slot_acquired = True
                logger.warning("%s got browser slot, starting search", source)

            # Phase 2: run the actual search (hard timeout)
            result = await asyncio.wait_for(
                client.search_flights(req), timeout=_search_timeout,
            )
            for offer in result.offers:
                offer.source = source
                offer.source_tier = "free"
            return result
        except asyncio.TimeoutError:
            logger.warning("%s timed out after %ds", source, _search_timeout)
            return _empty
        except BaseException as exc:
            # Catch CancelledError / KeyboardInterrupt / any crash —
            # never let one connector take down the whole search.
            logger.warning("%s crashed: %s", source, type(exc).__name__)
            return FlightSearchResponse(
                search_id="", origin=req.origin, destination=req.destination,
                currency=req.currency, offers=[], total_results=0,
            )
        finally:
            try:
                await client.close()
            except Exception:
                pass
            if uses_browser and slot_acquired:
                # Close module-level browser globals immediately so Chrome
                # doesn't linger until the full search completes.
                try:
                    await self._cleanup_single_connector(client)
                except Exception:
                    pass
                from connectors.browser import release_browser_slot
                release_browser_slot()

    def _combo_search_fn(self, label: str):
        """Return the appropriate search method for combo one-way legs."""
        mapping = {
            "ryanair_direct": self._search_ryanair_direct,
            "wizzair_direct": self._search_wizzair_direct,
            "kiwi_connector": self._search_kiwi_connector,
        }
        return mapping[label]

    # City code → constituent airport codes (multi-airport cities)
    _CITY_AIRPORTS: dict[str, set[str]] = {
        "LON": {"LHR", "LGW", "STN", "LCY", "LTN", "SEN"},
        "NYC": {"JFK", "LGA", "EWR"},
        "PAR": {"CDG", "ORY"},
        "MIL": {"MXP", "LIN", "BGY"},
        "TYO": {"NRT", "HND"},
        "OSA": {"KIX", "ITM"},
        "MOW": {"SVO", "DME", "VKO"},
        "BUE": {"EZE", "AEP"},
        "SAO": {"GRU", "CGH", "VCP"},
        "WAS": {"IAD", "DCA", "BWI"},
        "CHI": {"ORD", "MDW"},
        "SEL": {"ICN", "GMP"},
        "BJS": {"PEK", "PKX"},
        "SHA": {"PVG", "SHA"},
        "STO": {"ARN", "BMA", "NYO"},
        "ROM": {"FCO", "CIA"},
        "DXB": {"DXB", "DWC"},
        "IST": {"IST", "SAW"},
        "BKK": {"BKK", "DMK"},
        "JKT": {"CGK", "HLP"},
        "KUL": {"KUL", "SZB"},
        "RIO": {"GIG", "SDU"},
        "MEX": {"MEX", "NLU"},
        "YTO": {"YYZ", "YTZ", "YHM"},
        "YMQ": {"YUL", "YMX"},
    }

    # City code → primary (largest) airport for connectors that don't handle city codes
    _PRIMARY_AIRPORT: dict[str, str] = {
        "LON": "LHR", "NYC": "JFK", "PAR": "CDG", "MIL": "MXP",
        "TYO": "NRT", "OSA": "KIX", "MOW": "SVO", "BUE": "EZE",
        "SAO": "GRU", "WAS": "IAD", "CHI": "ORD", "SEL": "ICN",
        "BJS": "PEK", "SHA": "PVG", "STO": "ARN", "ROM": "FCO",
        "DXB": "DXB", "IST": "IST", "BKK": "BKK", "JKT": "CGK",
        "KUL": "KUL", "RIO": "GIG", "MEX": "MEX", "YTO": "YYZ", "YMQ": "YUL",
    }

    # Connectors that natively handle city codes — do NOT rewrite for these
    _CITY_CODE_AWARE: set[str] = {
        "kiwi_connector",
        "britishairways_direct",
        "virginatlantic_direct",
        "omanair_direct",
        # Connectors with internal get_city_airports() expansion
        "easyjet_direct",
        "jet2_direct",
        "level_direct",
        "airfrance_direct",
        "american_direct",
        "delta_direct",
        "eurowings_direct",
        "norwegian_direct",
        "pegasus_direct",
        "aerlingus_direct",
        "tap_direct",
        # Meta-search engines that natively support city codes in URLs
        "skyscanner_meta",
        "momondo_meta",
        "kayak_meta",
        "cheapflights_meta",
    }

    @classmethod
    def _resolve_primary(cls, code: str) -> str:
        """Resolve a city IATA code to its primary airport code.

        Returns the code unchanged if it's already an airport code.
        """
        return cls._PRIMARY_AIRPORT.get(code.upper(), code)

    def _expand_iata(self, code: str) -> set[str]:
        """Expand a city IATA code to its airports; single airports return themselves."""
        code = code.strip().upper()
        if code in self._CITY_AIRPORTS:
            return self._CITY_AIRPORTS[code]
        return {code}

    def _filter_wrong_routes(self, offers: list[FlightOffer], req: FlightSearchRequest) -> list[FlightOffer]:
        """Remove offers whose actual route doesn't match the requested origin → destination."""
        valid_origins = self._expand_iata(req.origin)
        valid_dests = self._expand_iata(req.destination)

        kept = []
        removed = 0
        for o in offers:
            if o.outbound and o.outbound.segments:
                first_seg = o.outbound.segments[0]
                last_seg = o.outbound.segments[-1]
                if first_seg.origin not in valid_origins or last_seg.destination not in valid_dests:
                    removed += 1
                    logger.debug("Route filter: dropped %s %s→%s (expected %s→%s)",
                                 o.owner_airline, first_seg.origin, last_seg.destination,
                                 req.origin, req.destination)
                    continue
            kept.append(o)

        if removed:
            logger.info("Route validation removed %d offers with wrong origin/destination (%d remain)",
                        removed, len(kept))
        return kept

    @staticmethod
    def _filter_legs_by_date(legs: list[FlightOffer], target_date) -> list[FlightOffer]:
        """Keep only legs whose departure date matches `target_date` (±1 day tolerance)."""
        from datetime import date, datetime, timedelta

        if target_date is None:
            return legs
        if isinstance(target_date, str):
            try:
                target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
            except ValueError:
                return legs
        if isinstance(target_date, datetime):
            target_date = target_date.date()

        kept = []
        removed = 0
        for leg in legs:
            if not leg.outbound or not leg.outbound.segments:
                kept.append(leg)
                continue
            dep = leg.outbound.segments[0].departure
            if dep is None:
                kept.append(leg)
                continue
            dep_date = dep.date() if isinstance(dep, datetime) else dep
            if abs((dep_date - target_date).days) <= 1:
                kept.append(leg)
            else:
                removed += 1
        if removed:
            logger.info("Date filter: removed %d legs not on %s", removed, target_date)
        return kept

    def _deduplicate(self, offers: list[FlightOffer]) -> list[FlightOffer]:
        """
        Remove near-duplicate offers from different providers.

        Two offers are considered duplicates if they have:
        - Same airlines
        - Same departure time (within 10 min)
        - Same arrival time (within 10 min)

        When duplicates found, keep the cheaper one (using normalized price
        for correct cross-currency comparison).
        """
        if not offers:
            return []

        def _norm_price(o: FlightOffer) -> float:
            return o.price_normalized if o.price_normalized is not None else o.price

        seen: dict[str, FlightOffer] = {}
        for offer in offers:
            key = self._dedup_key(offer)
            if key in seen:
                # Keep the cheaper one (normalized for cross-currency comparison)
                if _norm_price(offer) < _norm_price(seen[key]):
                    seen[key] = offer
            else:
                seen[key] = offer

        return list(seen.values())

    @staticmethod
    async def _normalize_prices(offers: list[FlightOffer], target_currency: str) -> None:
        """Set price_normalized on every offer, converting to target_currency."""
        target = target_currency.upper()

        # Collect unique source currencies that need conversion
        source_currencies = {o.currency.upper() for o in offers if o.currency.upper() != target}

        # Pre-fetch rates for each source currency (max 1 API call each, cached)
        rates_cache: dict[str, dict[str, float]] = {}
        for cur in source_currencies:
            try:
                rates_cache[cur] = await fetch_rates(cur)
            except Exception:
                rates_cache[cur] = {}

        for offer in offers:
            src = offer.currency.upper()
            if src == target:
                offer.price_normalized = offer.price
            else:
                rates = rates_cache.get(src, {})
                if target in rates:
                    offer.price_normalized = offer.price * rates[target]
                else:
                    offer.price_normalized = _fallback_convert(offer.price, src, target)

    def _diverse_select(
        self, offers: list[FlightOffer], sort: str, limit: int
    ) -> list[FlightOffer]:
        """
        Select offers ensuring airline diversity.

        1. Sort all offers by requested criteria
        2. Pick the cheapest offer per airline first (guarantees diversity)
        3. Fill remaining slots with cheapest overall
        """
        if not offers:
            return []

        # Sort
        def _sort_price(o: FlightOffer) -> float:
            return o.price_normalized if o.price_normalized is not None else o.price

        if sort == "duration":
            offers.sort(key=lambda o: o.outbound.total_duration_seconds if o.outbound else 0)
        else:
            # Default: price (normalized)
            offers.sort(key=_sort_price)

        # Step 1: Pick cheapest per airline (owner_airline)
        best_per_airline: dict[str, FlightOffer] = {}
        for offer in offers:
            key = offer.owner_airline or (offer.airlines[0] if offer.airlines else "?")
            if key not in best_per_airline:
                best_per_airline[key] = offer

        # Step 2: Build result — airline bests first, then fill by sort order
        selected_ids: set[str] = set()
        result: list[FlightOffer] = []

        # Add airline bests (sorted by sort criteria)
        airline_bests = sorted(
            best_per_airline.values(),
            key=lambda o: _sort_price(o) if sort != "duration" else (o.outbound.total_duration_seconds if o.outbound else 0),
        )
        for offer in airline_bests:
            if len(result) >= limit:
                break
            result.append(offer)
            selected_ids.add(offer.id)

        # Fill remaining with overall sorted offers
        for offer in offers:
            if len(result) >= limit:
                break
            if offer.id not in selected_ids:
                result.append(offer)
                selected_ids.add(offer.id)

        return result

    def _build_airlines_summary(self, offers: list[FlightOffer]) -> list[AirlineSummary]:
        """Build a summary of cheapest offer per airline."""
        by_airline: dict[str, list[FlightOffer]] = {}
        for offer in offers:
            key = offer.owner_airline or (offer.airlines[0] if offer.airlines else "?")
            if key not in by_airline:
                by_airline[key] = []
            by_airline[key].append(offer)

        summaries = []
        for airline_code, airline_offers in by_airline.items():
            airline_offers.sort(key=lambda o: o.price_normalized if o.price_normalized is not None else o.price)
            cheapest = airline_offers[0]

            # Build sample route string
            route_parts = []
            if cheapest.outbound and cheapest.outbound.segments:
                for seg in cheapest.outbound.segments:
                    route_parts.append(seg.origin)
                route_parts.append(cheapest.outbound.segments[-1].destination)
            sample_route = "→".join(route_parts)

            # Get airline name from first segment
            airline_name = ""
            if cheapest.outbound and cheapest.outbound.segments:
                airline_name = cheapest.outbound.segments[0].airline_name

            summaries.append(AirlineSummary(
                airline_code=airline_code,
                airline_name=airline_name,
                cheapest_price=cheapest.price,
                currency=cheapest.currency,
                offer_count=len(airline_offers),
                cheapest_offer_id=cheapest.id,
                sample_route=sample_route,
            ))

        summaries.sort(key=lambda s: s.cheapest_price)
        return summaries

    @staticmethod
    def _dedup_key(offer: FlightOffer) -> str:
        """Generate a deduplication key based on route and timing."""
        parts = []
        if offer.outbound and offer.outbound.segments:
            first = offer.outbound.segments[0]
            last = offer.outbound.segments[-1]
            # Round departure to nearest 10 minutes for fuzzy matching
            dep_rounded = first.departure.replace(
                minute=(first.departure.minute // 10) * 10, second=0, microsecond=0
            )
            arr_rounded = last.arrival.replace(
                minute=(last.arrival.minute // 10) * 10, second=0, microsecond=0
            )
            parts.append(f"{first.origin}-{last.destination}-{dep_rounded.isoformat()}-{arr_rounded.isoformat()}")
            # Add airlines
            airline_str = "-".join(sorted(s.airline for s in offer.outbound.segments))
            parts.append(airline_str)

        if offer.inbound and offer.inbound.segments:
            first = offer.inbound.segments[0]
            last = offer.inbound.segments[-1]
            dep_rounded = first.departure.replace(
                minute=(first.departure.minute // 10) * 10, second=0, microsecond=0
            )
            parts.append(f"ret-{dep_rounded.isoformat()}")

        return "|".join(parts) if parts else offer.id

    # ── Location resolution ──────────────────────────────────────────────

    async def resolve_location(self, query: str) -> dict:
        """Resolve location via the Cloud Run backend."""
        if not self.backend_available:
            return {"locations": [], "error": "No LETSFG_API_KEY configured"}

        url = f"{self._BACKEND_URL}/api/v1/flights/locations/{query}"
        headers = {
            "X-API-Key": self._BACKEND_KEY,
            "User-Agent": "letsfg-engine/1.0",
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()

    # ── Active providers info ────────────────────────────────────────────

    @property
    def active_providers(self) -> list[str]:
        providers = ["ryanair_direct", "wizzair_direct", "kiwi_connector"]
        providers.extend(name for name, _, _ in _DIRECT_AIRLINE_connectorS)
        if self.backend_available:
            providers.append("backend (Duffel, Amadeus, Sabre, Travelport, Kiwi, MCP, Trip.com)")
        return providers


# Singleton
multi_provider = MultiProvider()
