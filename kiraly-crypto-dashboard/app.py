import html
import os
import platform
from pathlib import Path

import pandas as pd
import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo


from collectors import get_collector, get_market_data_source_links, get_supported_exchange_names
from db import DB_PATH, connect, ensure_seed_data, get_fee_row, init_db, list_exchanges
from fees_service import (
    ServiceError,
    add_exchange_with_defaults,
    build_comparison_dataframe,
    delete_exchange_cascade,
    fetch_and_store_bitvavo_quote,
    save_exchange_details,
    save_exchange_fees,
)

DEFAULT_LANGUAGE = "nl"
LANGUAGE_OPTIONS = {
    "nl": "Nederlands",
    "en": "English",
}

TRANSLATIONS = {
    "nl": {
        "page_title": "KiralyAI | Crypto Exchange Kosten Dashboard",
        "language_label": "Taal",
        "hero_chip": "Live marktoverzicht",
        "hero_title": "Crypto Exchange Kosten Dashboard",
        "hero_subtitle": "Vergelijk de totale EUR-kosten van BTC kopen bij grote exchanges. Zie direct waar je het voordeligst uit bent, met live spread en fee-impact.",
        "hero_credit_prefix": "Dit dashboard is ontwikkeld in opdracht van ",
        "hero_credit_studio_label": "Studio Crypto",
        "hero_credit_middle": " en gerealiseerd door ",
        "hero_credit_kiraly_label": "KiralyAI",
        "hero_credit_suffix": ". Wij bouwen AI-oplossingen, dashboards en automatiseringen die bedrijven helpen sneller en slimmer te werken.",
        "pair_label": "Handelspaar",
        "investment_amount_label": "Investeringsbedrag",
        "market_data_label": "Marktdata",
        "refresh_market_data_button": "Marktdata verversen",
        "refresh_success": "Marktdata bijgewerkt voor: {exchanges}",
        "some_exchanges_failed": "Sommige exchanges mislukten: {details}",
        "admin_title": "Admin: fees en links",
        "fee_editor_expander": "Open fee-editor",
        "add_exchange_heading": "Exchange toevoegen (admin)",
        "name_label": "Naam",
        "type_label": "Type",
        "website_label": "Website",
        "affiliate_url_label": "Affiliate URL",
        "add_exchange_button": "Exchange toevoegen",
        "exchange_added_success": "Exchange toegevoegd: {name}",
        "load_exchanges_error": "Kon exchanges niet laden: {error}",
        "no_exchanges_found": "Geen exchanges gevonden. Voeg er hierboven een toe.",
        "exchange_label": "Exchange",
        "edit_exchange_heading": "Exchange bewerken",
        "maker_fee_pct_label": "Maker fee %",
        "maker_fee_help": "Wordt getoond voor transparantie, maar niet gebruikt in de total-cost ranking.",
        "taker_fee_pct_label": "Taker fee %",
        "taker_fee_help": "Wordt gebruikt voor de total-cost ranking en gespiegeld naar trading_fee_pct.",
        "spread_estimate_pct_label": "Spread-inschatting %",
        "spread_estimate_help": "Fallback-spread als er geen live orderbook quote is.",
        "ideal_fee_eur_label": "iDEAL stortingsfee (€)",
        "eur_withdrawal_fee_label": "EUR opnamefee (€)",
        "fee_source_url_label": "Feebron URL",
        "save_exchange_settings_button": "Exchange-instellingen opslaan",
        "save_exchange_settings_success": "Exchange-instellingen opgeslagen voor {name}",
        "delete_exchange_heading": "Exchange verwijderen",
        "delete_exchange_help": "Verwijder alleen exchanges die je niet meer wilt vergelijken.",
        "exchange_to_delete_label": "Te verwijderen exchange",
        "confirm_delete_label": "Ik begrijp dat dit gerelateerde fees en quotes verwijdert.",
        "delete_exchange_button": "Exchange verwijderen",
        "confirm_delete_warning": "Bevestig eerst het verwijderen voordat je doorgaat.",
        "delete_exchange_success": "Exchange verwijderd: {name}",
        "live_badge": "Live",
        "fallback_badge": "Fallback",
        "best_price_badge": "Beste prijs",
        "rank_label": "Rang #{rank}",
        "cheapest_exchange_label": "Goedkoopste exchange (obv taker fee)",
        "total_cost_suffix": "totale kosten",
        "lowest_total_cost_label": "Laagste totale kosten (obv taker fee)",
        "based_on_amount": "Gebaseerd op € {amount}",
        "best_spread_label": "Beste spread",
        "latest_market_update_label": "Laatste marktupdate",
        "most_recent_visible_quote_label": "Meest recente zichtbare quote",
        "no_comparison_data": "Geen vergelijkingsdata beschikbaar.",
        "ranked_exchanges_title": "Gerangschikte exchanges",
        "ranked_exchanges_subtitle": "Een overzichtelijker beeld van de totale BTC-aankoopkosten over alle ondersteunde platforms.",
        "total_cost_on_amount": "Totale kosten op €{amount} (obv taker fee)",
        "cheapest_total_cost_summary": "{cost} totale kosten gebaseerd op €{amount}",
        "maker_fee_pct_short": "Maker fee %",
        "taker_fee_pct_short": "Taker fee %",
        "used_fee_pct_short": "Gebruikte fee %",
        "spread_pct_short": "Spread %",
        "total_pct_short": "Totaal %",
        "quote_source_label": "Quotebron",
        "details_expander_label": "Gedetailleerde vergelijkingsdata openen",
        "export_csv_button": "CSV exporteren",
        "status_label": "Status",
        "status_cheapest": "Goedkoopste",
        "exchange_col": "Exchange",
        "type_col": "Type",
        "type_value_exchange": "Exchange",
        "type_value_broker": "Broker",
        "ideal_fee_col": "iDEAL fee €",
        "eur_withdrawal_fee_col": "EUR opname €",
        "fees_updated_col": "Fees bijgewerkt",
        "api_source_col": "API-bron",
        "fx_reference_col": "FX-referentie",
        "fee_source_col": "Feebron",
        "total_eur_col": "Totale kosten op €{amount} (obv taker fee)",
        "debug_title": "Debug",
        "db_label": "DB:",
        "streamlit_cloud_mode_label": "Streamlit Cloud-modus:",
        "live_exchanges_label": "Live exchanges:",
        "exchanges_label": "Exchanges:",
        "quote_count_col": "Aantal quotes",
        "latest_ts_col": "Laatste ts",
        "live_quote_refresh_issues": "Problemen bij live quote refresh: {details}",
        "unexpected_dashboard_error": "Onverwachte fout tijdens het bouwen van het dashboard: {error}",
        "admin_tip_caption": "Tip: de totale ranking gebruikt taker fees uit de database, terwijl spreads en bronlinks live worden ververst.",
        "costs_disclaimer": "De weergegeven kosten worden live opgehaald bij de betreffende exchanges en kunnen afwijken van de werkelijk gehanteerde tarieven. Aan deze informatie kunnen geen rechten worden ontleend. Controleer altijd de actuele kosten rechtstreeks bij de exchange voordat u een transactie uitvoert.",
        "studio_crypto_credit_prefix": "In opdracht van ",
        "studio_crypto_credit_label": "Studio Crypto",
        "source_live": "Live",
        "source_fallback": "Fallback",
        "source_estimate": "Schatting",
        "error_exchange_exists": "Exchange bestaat al: {name}",
        "error_add_exchange": "Kon exchange niet toevoegen: {error}",
        "error_save_fees": "Kon fees niet opslaan: {error}",
        "error_save_exchange_details": "Kon exchangegegevens niet opslaan: {error}",
        "error_exchange_not_found": "Exchange niet gevonden.",
        "error_delete_exchange": "Kon exchange niet verwijderen: {error}",
        "error_exchange_missing_seed": "{name} staat niet in exchanges. Run scripts/init_db.py",
        "error_fetch_store_quote": "Kon quote voor {name} niet ophalen/opslaan: {error}",
        "footer_primary": "Ontwikkeld door KiralyAI - AI-tools, automatiseringen en moderne websites voor groeiende bedrijven.",
        "footer_secondary_prefix": "Meer projecten en oplossingen bekijken? Bezoek ",
        "footer_secondary_link_label": "KiralyAI",
        "footer_secondary_suffix": ".",
    },
    "en": {
        "page_title": "KiralyAI | Crypto Exchange Cost Dashboard",
        "language_label": "Language",
        "hero_chip": "Live market overview",
        "hero_title": "Crypto Exchange Cost Dashboard",
        "hero_subtitle": "Compare the total EUR cost of buying BTC across major exchanges. See the cheapest venue instantly, with live spread and fee impact.",
        "hero_credit_prefix": "This dashboard was developed on behalf of ",
        "hero_credit_studio_label": "Studio Crypto",
        "hero_credit_middle": " and delivered by ",
        "hero_credit_kiraly_label": "KiralyAI",
        "hero_credit_suffix": ". We build AI solutions, dashboards, and automations that help businesses work faster and smarter.",
        "pair_label": "Pair",
        "investment_amount_label": "Investment amount",
        "market_data_label": "Market data",
        "refresh_market_data_button": "Refresh market data",
        "refresh_success": "Updated market data for: {exchanges}",
        "some_exchanges_failed": "Some exchanges failed: {details}",
        "admin_title": "Admin: fees and links",
        "fee_editor_expander": "Open fee editor",
        "add_exchange_heading": "Add exchange (admin)",
        "name_label": "Name",
        "type_label": "Type",
        "website_label": "Website",
        "affiliate_url_label": "Affiliate URL",
        "add_exchange_button": "Add exchange",
        "exchange_added_success": "Added exchange: {name}",
        "load_exchanges_error": "Could not load exchanges: {error}",
        "no_exchanges_found": "No exchanges found. Add one above.",
        "exchange_label": "Exchange",
        "edit_exchange_heading": "Edit exchange",
        "maker_fee_pct_label": "Maker fee %",
        "maker_fee_help": "Shown for transparency, but not used in total-cost ranking.",
        "taker_fee_pct_label": "Taker fee %",
        "taker_fee_help": "Used for total-cost ranking and mirrored into trading_fee_pct.",
        "spread_estimate_pct_label": "Spread estimate %",
        "spread_estimate_help": "Fallback spread when no live orderbook quote is available.",
        "ideal_fee_eur_label": "iDEAL deposit fee (€)",
        "eur_withdrawal_fee_label": "EUR withdrawal fee (€)",
        "fee_source_url_label": "Fee source URL",
        "save_exchange_settings_button": "Save exchange settings",
        "save_exchange_settings_success": "Saved exchange settings for {name}",
        "delete_exchange_heading": "Delete exchange",
        "delete_exchange_help": "Only remove exchanges you no longer want to compare.",
        "exchange_to_delete_label": "Exchange to delete",
        "confirm_delete_label": "I understand this will delete related fees and quotes.",
        "delete_exchange_button": "Delete exchange",
        "confirm_delete_warning": "Please confirm delete before continuing.",
        "delete_exchange_success": "Deleted exchange: {name}",
        "live_badge": "Live",
        "fallback_badge": "Fallback",
        "best_price_badge": "Best price",
        "rank_label": "Rank #{rank}",
        "cheapest_exchange_label": "Cheapest exchange (based on taker fee)",
        "total_cost_suffix": "total cost",
        "lowest_total_cost_label": "Lowest total cost (based on taker fee)",
        "based_on_amount": "Based on € {amount}",
        "best_spread_label": "Best spread",
        "latest_market_update_label": "Latest market update",
        "most_recent_visible_quote_label": "Most recent visible quote",
        "no_comparison_data": "No comparison data available.",
        "ranked_exchanges_title": "Ranked exchanges",
        "ranked_exchanges_subtitle": "A cleaner view of the total BTC purchase cost across all supported venues.",
        "total_cost_on_amount": "Total cost on €{amount} (based on taker fee)",
        "cheapest_total_cost_summary": "{cost} total cost based on €{amount}",
        "maker_fee_pct_short": "Maker fee %",
        "taker_fee_pct_short": "Taker fee %",
        "used_fee_pct_short": "Used fee %",
        "spread_pct_short": "Spread %",
        "total_pct_short": "Total %",
        "quote_source_label": "Quote source",
        "details_expander_label": "Open detailed comparison data",
        "export_csv_button": "Export CSV",
        "status_label": "Status",
        "status_cheapest": "Cheapest",
        "exchange_col": "Exchange",
        "type_col": "Type",
        "type_value_exchange": "Exchange",
        "type_value_broker": "Broker",
        "ideal_fee_col": "iDEAL fee €",
        "eur_withdrawal_fee_col": "EUR withdrawal €",
        "fees_updated_col": "Fees updated",
        "api_source_col": "API source",
        "fx_reference_col": "FX reference",
        "fee_source_col": "Fee source",
        "total_eur_col": "Total cost on €{amount} (based on taker fee)",
        "debug_title": "Debug",
        "db_label": "DB:",
        "streamlit_cloud_mode_label": "Streamlit Cloud mode:",
        "live_exchanges_label": "Live exchanges:",
        "exchanges_label": "Exchanges:",
        "quote_count_col": "Quote count",
        "latest_ts_col": "Latest ts",
        "live_quote_refresh_issues": "Live quote refresh issues: {details}",
        "unexpected_dashboard_error": "Unexpected error while building dashboard: {error}",
        "admin_tip_caption": "Tip: total-cost ranking uses taker fees from the database, while spreads and source links are refreshed from live data.",
        "costs_disclaimer": "The displayed costs are retrieved live from the relevant exchanges and may differ from the actual rates applied. No rights can be derived from this information. Always verify the current fees directly with the exchange before executing a transaction.",
        "studio_crypto_credit_prefix": "Commissioned by ",
        "studio_crypto_credit_label": "Studio Crypto",
        "source_live": "Live",
        "source_fallback": "Fallback",
        "source_estimate": "Estimate",
        "error_exchange_exists": "Exchange already exists: {name}",
        "error_add_exchange": "Could not add exchange: {error}",
        "error_save_fees": "Could not save fees: {error}",
        "error_save_exchange_details": "Could not save exchange details: {error}",
        "error_exchange_not_found": "Exchange not found.",
        "error_delete_exchange": "Could not delete exchange: {error}",
        "error_exchange_missing_seed": "{name} is not in exchanges. Run scripts/init_db.py",
        "error_fetch_store_quote": "Could not fetch/store {name} quote: {error}",
        "footer_primary": "Built by KiralyAI — AI tools, automations, and modern websites for growing businesses.",
        "footer_secondary_prefix": "Explore more projects and solutions at ",
        "footer_secondary_link_label": "KiralyAI",
        "footer_secondary_suffix": ".",
    },
}


def _init_language_state() -> None:
    if st.session_state.get("language") not in LANGUAGE_OPTIONS:
        st.session_state["language"] = DEFAULT_LANGUAGE


def _current_language() -> str:
    language = st.session_state.get("language", DEFAULT_LANGUAGE)
    return language if language in LANGUAGE_OPTIONS else DEFAULT_LANGUAGE


def t(key: str, **kwargs) -> str:
    _init_language_state()
    language = _current_language()
    template = TRANSLATIONS.get(language, TRANSLATIONS[DEFAULT_LANGUAGE]).get(
        key,
        TRANSLATIONS[DEFAULT_LANGUAGE].get(key, key),
    )
    try:
        return str(template).format(**kwargs)
    except Exception:
        return str(template)


def _translate_service_error_message(message: object) -> str:
    raw = str(message or "").strip()
    if not raw:
        return ""

    if raw == "Exchange not found.":
        return t("error_exchange_not_found")

    if raw.endswith(" staat niet in exchanges. Run scripts/init_db.py"):
        exchange_name = raw.split(" staat niet in exchanges. Run scripts/init_db.py", 1)[0]
        return t("error_exchange_missing_seed", name=exchange_name)

    if raw.startswith("Exchange already exists: "):
        return t("error_exchange_exists", name=raw.split(": ", 1)[1])

    if raw.startswith("Could not add exchange: "):
        return t("error_add_exchange", error=raw.split(": ", 1)[1])

    if raw.startswith("Could not save fees: "):
        return t("error_save_fees", error=raw.split(": ", 1)[1])

    if raw.startswith("Could not save exchange details: "):
        return t("error_save_exchange_details", error=raw.split(": ", 1)[1])

    if raw.startswith("Could not delete exchange: "):
        return t("error_delete_exchange", error=raw.split(": ", 1)[1])

    if raw.startswith("Could not fetch/store ") and " quote: " in raw:
        remainder = raw[len("Could not fetch/store "):]
        exchange_name, error = remainder.split(" quote: ", 1)
        return t("error_fetch_store_quote", name=exchange_name, error=error)

    return raw


_init_language_state()

LIVE_EXCHANGES = ("Bitvavo", "Kraken", "Coinbase", "Bybit", "OKX")

st.set_page_config(
    page_title=t("page_title"),
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def apply_light_style() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: linear-gradient(180deg, #F7F9FC 0%, #EEF3F8 100%);
            color: #0F172A;
        }

        .block-container {
            max-width: 1240px;
            padding-top: 0.2rem;
            padding-bottom: 2rem;
        }

        h1, h2, h3, h4 {
            margin-top: 0;
            margin-bottom: 0.35rem;
            color: #0F172A;
        }

        [data-testid="stVerticalBlockBorderWrapper"] {
            background: rgba(255,255,255,0.82);
            border: 1px solid rgba(255,255,255,0.82);
            border-radius: 20px;
            box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
            backdrop-filter: blur(10px);
        }

        [data-testid="stVerticalBlockBorderWrapper"] > div {
            padding: 20px 22px;
        }

        [data-testid="stButton"] > button,
        [data-testid="stDownloadButton"] > button {
            background: linear-gradient(135deg, #0EA5E9 0%, #2563EB 100%) !important;
            color: white !important;
            border: none !important;
            border-radius: 14px !important;
            font-weight: 700 !important;
            min-height: 48px;
        }

        [data-testid="stButton"] > button:hover,
        [data-testid="stDownloadButton"] > button:hover {
            filter: brightness(1.03);
            transform: translateY(-1px);
        }

        [data-testid="stButton"] > button:disabled,
        [data-testid="stDownloadButton"] > button:disabled {
            opacity: 0.65;
            cursor: not-allowed !important;
        }

        [data-baseweb="input"] input,
        [data-baseweb="select"] input,
        [data-baseweb="select"] span,
        [data-baseweb="select"] div,
        [data-baseweb="textarea"] textarea {
            color: #0F172A !important;
        }

        [data-baseweb="input"] > div,
        [data-baseweb="select"] > div,
        [data-baseweb="textarea"] > div {
            background: #FFFFFF !important;
            border-color: #D7DFEA !important;
            border-radius: 14px !important;
        }

        [data-baseweb="input"] input::placeholder,
        [data-baseweb="textarea"] textarea::placeholder {
            color: #64748B !important;
            opacity: 1 !important;
        }

        .hero-wrap {
            padding: 0 0 4px 0;
        }

        .hero-chip {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 999px;
            background: rgba(14, 165, 233, 0.12);
            color: #0369A1;
            font-size: 0.82rem;
            font-weight: 700;
            margin-bottom: 14px;
        }

        .hero-title {
            font-size: 2rem;
            font-weight: 800;
            line-height: 1.05;
            color: #0F172A;
            margin-bottom: 8px;
        }

        .hero-subtitle {
            color: #64748B;
            font-size: 1rem;
            max-width: 760px;
        }

        .hero-credit {
            color: #64748B;
            font-size: 0.96rem;
            line-height: 1.7;
            max-width: 760px;
            margin-top: 10px;
        }

        .hero-credit a {
            color: #1D4ED8;
            text-decoration: none;
            font-weight: 700;
        }

        .hero-credit a:hover {
            text-decoration: underline;
        }

        .control-label {
            color: #475569;
            font-weight: 700;
            font-size: 0.82rem;
            margin-bottom: 6px;
        }

        .metric-card {
            background: linear-gradient(180deg, rgba(255,255,255,0.96) 0%, rgba(248,250,252,0.96) 100%);
            border: 1px solid #E2E8F0;
            border-radius: 18px;
            padding: 18px;
            box-shadow: 0 6px 20px rgba(15, 23, 42, 0.05);
            min-height: 112px;
        }

        .metric-label {
            color: #64748B;
            font-size: 0.8rem;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 10px;
        }

        .metric-value {
            font-size: 1.8rem;
            font-weight: 800;
            color: #0F172A;
            line-height: 1.05;
        }

        .metric-subvalue {
            color: #475569;
            font-size: 0.95rem;
            margin-top: 8px;
        }

        .section-title {
            font-size: 1.35rem;
            font-weight: 800;
            color: #0F172A;
            margin-bottom: 10px;
        }

        .section-subtitle {
            color: #64748B;
            font-size: 0.95rem;
            margin-bottom: 6px;
        }

        .exchange-card {
            background: rgba(255,255,255,0.94);
            border: 1px solid #E2E8F0;
            border-radius: 20px;
            padding: 20px;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
            margin-bottom: 14px;
        }

        .exchange-top {
            display: flex;
            justify-content: space-between;
            align-items: start;
            gap: 16px;
            margin-bottom: 14px;
        }

        .exchange-rank {
            font-size: 0.78rem;
            font-weight: 800;
            color: #0369A1;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            margin-bottom: 6px;
        }

        .exchange-name {
            font-size: 1.35rem;
            font-weight: 800;
            color: #0F172A;
            margin-bottom: 2px;
        }

        .exchange-meta {
            color: #64748B;
            font-size: 0.92rem;
        }

        .exchange-total {
            text-align: right;
        }

        .exchange-total-label {
            color: #64748B;
            font-size: 0.78rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .exchange-total-value {
            font-size: 1.7rem;
            font-weight: 800;
            color: #0F172A;
            line-height: 1.1;
            margin-top: 4px;
        }

        .exchange-grid {
            display: grid;
            grid-template-columns: repeat(6, minmax(0, 1fr));
            gap: 12px;
            margin-top: 10px;
        }

        .mini-stat {
            background: #F8FAFC;
            border: 1px solid #E2E8F0;
            border-radius: 14px;
            padding: 12px;
        }

        .mini-stat-label {
            color: #64748B;
            font-size: 0.75rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            margin-bottom: 6px;
        }

        .mini-stat-value {
            color: #0F172A;
            font-size: 1rem;
            font-weight: 800;
        }

        .badge {
            display: inline-block;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 0.76rem;
            font-weight: 800;
            margin-right: 8px;
        }

        .badge-best {
            background: #DCFCE7;
            color: #166534;
        }

        .badge-live {
            background: #DBEAFE;
            color: #1D4ED8;
        }

        .badge-fallback {
            background: #FEF3C7;
            color: #92400E;
        }

        .kiraly-subtle {
            color: #64748B;
            font-size: 0.95rem;
        }

        .exchange-links {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-top: 14px;
        }

        .exchange-link {
            display: inline-flex;
            align-items: center;
            padding: 7px 11px;
            background: #EFF6FF;
            border: 1px solid #BFDBFE;
            border-radius: 999px;
        }

        .exchange-link a {
            color: #1D4ED8;
            font-size: 0.84rem;
            font-weight: 700;
            text-decoration: none;
        }

        .exchange-link a:hover {
            text-decoration: underline;
        }

        .site-footer {
            margin-top: 52px;
            padding: 26px 12px 6px;
            border-top: 1px solid #D7DFEA;
            text-align: center;
        }

        .site-footer-primary {
            color: #64748B;
            font-size: 0.92rem;
            line-height: 1.6;
        }

        .site-footer-secondary {
            color: #94A3B8;
            font-size: 0.82rem;
            margin-top: 6px;
            line-height: 1.6;
        }

        .site-footer-secondary a {
            color: #475569;
            text-decoration: none;
            font-weight: 700;
        }

        .site-footer-secondary a:hover {
            text-decoration: underline;
        }

        .kiraly-disclaimer {
            margin-top: 20px;
            padding: 16px 18px;
            border: 1px solid #E2E8F0;
            border-radius: 16px;
            background: rgba(248, 250, 252, 0.96);
        }

        .kiraly-disclaimer-text {
            color: #64748B;
            font-size: 0.84rem;
            line-height: 1.65;
        }

        .client-credit {
            margin-top: 14px;
            text-align: center;
            color: #94A3B8;
            font-size: 0.82rem;
            line-height: 1.6;
        }

        .client-credit a {
            color: #475569;
            text-decoration: none;
            font-weight: 700;
        }

        .client-credit a:hover {
            text-decoration: underline;
        }

        div[data-testid="stDataFrame"] {
            border-radius: 16px;
            overflow: hidden;
            border: 1px solid #E2E8F0;
        }

        div[data-testid="stDataFrame"] [role="grid"] {
            background: #FFFFFF !important;
            color: #0F172A !important;
        }

        section[data-testid="stSidebar"] { display: none !important; }
        button[kind="header"] { display: none !important; }
        #MainMenu { visibility: hidden; }
        footer { visibility: hidden; }

        header[data-testid="stHeader"] {
            display: none !important;
            height: 0 !important;
        }

        [data-testid="stToolbar"] {
            display: none !important;
        }

        [data-testid="stDecoration"] {
            display: none !important;
        }

        [data-testid="stAppViewContainer"] > .main {
            padding-top: 0 !important;
            margin-top: 0 !important;
        }

        @media (max-width: 900px) {
            .exchange-grid {
                grid-template-columns: 1fr 1fr;
            }
            .exchange-top {
                flex-direction: column;
            }
            .exchange-total {
                text-align: left;
            }
            .block-container {
                padding-top: 0.1rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header() -> None:
    with st.container():
        left_col, center_col, right_col = st.columns([1, 6, 2.2], vertical_alignment="center")

        with left_col:
            try:
                st.image(str(Path("assets/kiraly-logo.png")), width=88)
            except Exception:
                pass

        with center_col:
            studio_url = html.escape("https://www.instagram.com/studiocrypto.nl", quote=True)
            kiraly_url = html.escape("https://kiralyai.com", quote=True)
            hero_credit_html = (
                html.escape(t("hero_credit_prefix"))
                + f'<a href="{studio_url}" target="_blank" rel="noopener noreferrer">{html.escape(t("hero_credit_studio_label"))}</a>'
                + html.escape(t("hero_credit_middle"))
                + f'<a href="{kiraly_url}" target="_blank" rel="noopener noreferrer">{html.escape(t("hero_credit_kiraly_label"))}</a>'
                + html.escape(t("hero_credit_suffix"))
            )
            st.markdown('<div class="hero-wrap">', unsafe_allow_html=True)
            st.markdown(f'<div class="hero-chip">{t("hero_chip")}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="hero-title">{t("hero_title")}</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="hero-subtitle">{t("hero_subtitle")}</div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div class="hero-credit">{hero_credit_html}</div>',
                unsafe_allow_html=True,
            )
            st.markdown("</div>", unsafe_allow_html=True)

        with right_col:
            st.markdown(
                f'<div class="control-label">{t("language_label")}</div>',
                unsafe_allow_html=True,
            )
            st.selectbox(
                t("language_label"),
                options=list(LANGUAGE_OPTIONS.keys()),
                format_func=lambda language_code: LANGUAGE_OPTIONS.get(language_code, language_code),
                key="language",
                label_visibility="collapsed",
            )


def is_streamlit_cloud() -> bool:
    if os.getenv("STREAMLIT_CLOUD"):
        return True
    if "streamlit" in platform.node().lower():
        return True
    return False


def _is_admin_mode() -> bool:
    try:
        qp = st.query_params
        if str(qp.get("admin", "")).strip() in ("1", "true", "True", "yes"):
            return True
    except Exception:
        pass

    try:
        return str(st.secrets.get("ADMIN_MODE", "false")).lower() == "true"
    except Exception:
        return False


def _get_dashboard_exchanges(con) -> list[str]:
    supported = set(get_supported_exchange_names())
    allowed = set(LIVE_EXCHANGES)
    rows = list_exchanges(con)
    return [
        str(row["name"])
        for row in rows
        if str(row["name"]) in supported and str(row["name"]) in allowed
    ]


def _refresh_live_quotes(
    con, symbol: str, exchange_names: list[str]
) -> tuple[list[str], dict[str, str], list[str]]:
    successes: list[str] = []
    failures: dict[str, str] = {}
    fallback_used: list[str] = []

    for exchange_name in exchange_names:
        try:
            collector = get_collector(exchange_name)
            fetch_and_store_bitvavo_quote(
                con,
                symbol=symbol,
                collector=collector,
                exchange_name=exchange_name,
            )
            successes.append(exchange_name)

            if getattr(collector, "last_quote_mode", "") == "fallback_btcusdt_usdteur":
                fallback_used.append(exchange_name)
        except Exception as exc:
            failures[exchange_name] = str(exc)
            print(f"[live-refresh] {exchange_name}: {exc}")

    return successes, failures, fallback_used


def render_controls(con):
    with st.container(border=True):
        c1, c2, c3 = st.columns([2, 2, 1.3], gap="small", vertical_alignment="bottom")

        with c1:
            st.markdown(
                f'<div class="control-label">{t("pair_label")}</div>',
                unsafe_allow_html=True,
            )
            symbol = st.selectbox(t("pair_label"), ["BTC-EUR"], label_visibility="collapsed")

        with c2:
            st.markdown(
                f'<div class="control-label">{t("investment_amount_label")}</div>',
                unsafe_allow_html=True,
            )
            amount = st.selectbox(
                t("investment_amount_label"),
                [100, 1000, 10000],
                index=1,
                label_visibility="collapsed",
            )

        with c3:
            st.markdown(
                f'<div class="control-label">{t("market_data_label")}</div>',
                unsafe_allow_html=True,
            )
            exchange_names_for_fetch = _get_dashboard_exchanges(con)
            fetch_clicked = st.button(
                t("refresh_market_data_button"),
                disabled=not exchange_names_for_fetch,
                use_container_width=True,
            )

        if fetch_clicked:
            refreshed, failed, fallback_used = _refresh_live_quotes(
                con, symbol=symbol, exchange_names=exchange_names_for_fetch
            )
            st.session_state["fallback_exchanges"] = fallback_used

            if refreshed:
                st.success(t("refresh_success", exchanges=", ".join(refreshed)))

            if failed:
                failed = {name: err for name, err in failed.items() if name in LIVE_EXCHANGES}
                if failed and ADMIN_MODE:
                    st.warning(
                        t(
                            "some_exchanges_failed",
                            details="; ".join(
                                [
                                    f"{name}: {_translate_service_error_message(error)}"
                                    for name, error in failed.items()
                                ]
                            ),
                        )
                    )

    return symbol, amount


def render_admin(con) -> None:
    st.subheader(t("admin_title"))

    with st.expander(t("fee_editor_expander"), expanded=False):
        with st.container(border=True):
            st.markdown(f"#### {t('add_exchange_heading')}")
            with st.form("add_exchange_form", clear_on_submit=True):
                new_exchange_name = st.text_input(t("name_label"), key="new_exchange_name")
                new_exchange_type = st.selectbox(
                    t("type_label"),
                    ["exchange", "broker"],
                    format_func=_translate_exchange_type,
                    key="new_exchange_type",
                )
                new_exchange_website = st.text_input(t("website_label"), key="new_exchange_website")
                new_exchange_affiliate_url = st.text_input(
                    t("affiliate_url_label"),
                    key="new_exchange_affiliate_url",
                )
                add_exchange_submitted = st.form_submit_button(t("add_exchange_button"))

            if add_exchange_submitted:
                try:
                    add_exchange_with_defaults(
                        con,
                        name=new_exchange_name,
                        exchange_type=new_exchange_type,
                        website=new_exchange_website,
                        affiliate_url=new_exchange_affiliate_url,
                    )
                    st.success(t("exchange_added_success", name=new_exchange_name.strip()))
                    st.rerun()
                except ServiceError as exc:
                    st.error(_translate_service_error_message(exc))

            try:
                exchanges = list_exchanges(con)
            except Exception as exc:
                exchanges = []
                st.error(t("load_exchanges_error", error=exc))

            if not exchanges:
                st.info(t("no_exchanges_found"))
                return

            ex_name_to_id = {str(ex["name"]): int(ex["id"]) for ex in exchanges}
            selected_name = st.selectbox(t("exchange_label"), list(ex_name_to_id.keys()))
            selected_id = ex_name_to_id[selected_name]
            selected_exchange = next(ex for ex in exchanges if int(ex["id"]) == selected_id)

            fee_row = get_fee_row(con, selected_id)
            if fee_row:
                maker_fee_pct = float(fee_row["maker_fee_pct"])
                taker_fee_pct = float(fee_row["taker_fee_pct"])
                deposit_ideal_fee_eur = float(fee_row["deposit_ideal_fee_eur"])
                withdraw_eur_fee_eur = float(fee_row["withdraw_eur_fee_eur"])
                spread_estimate_pct = float(fee_row["spread_estimate_pct"])
                source_url = str(fee_row["source_url"] or "")
            else:
                maker_fee_pct = 0.0
                taker_fee_pct = 0.0
                deposit_ideal_fee_eur = 0.0
                withdraw_eur_fee_eur = 0.0
                spread_estimate_pct = 0.0
                source_url = ""

            st.markdown(f"#### {t('edit_exchange_heading')}")
            profile_col_a, profile_col_b = st.columns(2)
            with profile_col_a:
                new_website = st.text_input(
                    t("website_label"),
                    value=str(selected_exchange["website"] or ""),
                )
            with profile_col_b:
                new_affiliate_url = st.text_input(
                    t("affiliate_url_label"),
                    value=str(selected_exchange["affiliate_url"] or ""),
                )

            col_a, col_b = st.columns(2)
            with col_a:
                new_maker_fee = st.number_input(
                    t("maker_fee_pct_label"),
                    value=maker_fee_pct,
                    step=0.01,
                    format="%.4f",
                    help=t("maker_fee_help"),
                )
                new_taker_fee = st.number_input(
                    t("taker_fee_pct_label"),
                    value=taker_fee_pct,
                    step=0.01,
                    format="%.4f",
                    help=t("taker_fee_help"),
                )
            with col_b:
                new_spread_est = st.number_input(
                    t("spread_estimate_pct_label"),
                    value=spread_estimate_pct,
                    step=0.01,
                    format="%.4f",
                    help=t("spread_estimate_help"),
                )

            new_source = st.text_input(t("fee_source_url_label"), value=source_url)

            if st.button(t("save_exchange_settings_button")):
                try:
                    save_exchange_details(
                        con,
                        exchange_id=selected_id,
                        name=str(selected_exchange["name"]),
                        exchange_type=str(selected_exchange["type"]),
                        website=new_website,
                        affiliate_url=new_affiliate_url,
                    )
                    save_exchange_fees(
                        con,
                        exchange_id=selected_id,
                        trading_fee_pct=float(new_taker_fee),
                        deposit_ideal_fee_eur=float(deposit_ideal_fee_eur),
                        withdraw_eur_fee_eur=float(withdraw_eur_fee_eur),
                        spread_estimate_pct=float(new_spread_est),
                        source_url=new_source,
                        maker_fee_pct=float(new_maker_fee),
                        taker_fee_pct=float(new_taker_fee),
                    )
                    st.success(t("save_exchange_settings_success", name=selected_name))
                    st.rerun()
                except ServiceError as exc:
                    st.error(_translate_service_error_message(exc))

            st.markdown(f"#### {t('delete_exchange_heading')}")
            st.markdown(
                f'<div class="kiraly-subtle">{t("delete_exchange_help")}</div>',
                unsafe_allow_html=True,
            )
            with st.form("delete_exchange_form"):
                delete_exchange_name = st.selectbox(
                    t("exchange_to_delete_label"),
                    list(ex_name_to_id.keys()),
                    key="delete_exchange_name",
                )
                confirm_delete = st.checkbox(
                    t("confirm_delete_label"),
                    key="confirm_delete_exchange",
                )
                delete_exchange_submitted = st.form_submit_button(t("delete_exchange_button"))

            if delete_exchange_submitted:
                if not confirm_delete:
                    st.warning(t("confirm_delete_warning"))
                else:
                    try:
                        delete_exchange_cascade(con, ex_name_to_id[delete_exchange_name])
                        st.success(t("delete_exchange_success", name=delete_exchange_name))
                        st.rerun()
                    except ServiceError as exc:
                        st.error(_translate_service_error_message(exc))


def _format_pct(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".") + "%"


def _format_spread_pct(value: float) -> str:
    spread_pct = abs(float(value))
    if 0 < spread_pct < 0.01:
        return "<0.01%"
    return _format_pct(float(value))


def _format_eur(value: float) -> str:
    return f"€ {float(value):,.2f}"


def _extract_live_ts(source_value: str) -> str:
    raw = str(source_value or "")
    if raw.startswith("live (") and raw.endswith(")"):
        return raw[len("live ("):-1]
    return ""


def _format_ts_short(value: str) -> str:
    if not value:
        return "—"

    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        dt = dt.astimezone(ZoneInfo("Europe/Amsterdam"))
        return dt.strftime("%Y-%m-%d %H:%M") + " CET"
    except Exception:
        return str(value)


def _normalize_link_url(value: object) -> str:
    url = str(value or "").strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return ""


def _build_link_html(label: str, url: str) -> str:
    safe_label = html.escape(label)
    safe_url = html.escape(url, quote=True)
    return (
        f'<a href="{safe_url}" target="_blank" rel="noopener noreferrer">{safe_label}</a>'
    )


def _translate_exchange_type(exchange_type: object) -> str:
    exchange_type = str(exchange_type or "").strip().lower()
    if exchange_type == "broker":
        return t("type_value_broker")
    return t("type_value_exchange")


def _translate_link_label(label: object) -> str:
    normalized = str(label or "").strip().lower()
    mapping = {
        "website": t("website_label"),
        "fx reference": t("fx_reference_col"),
        "fee source": t("fee_source_col"),
    }
    return mapping.get(normalized, str(label or ""))


def _is_api_source_label(label: object) -> bool:
    return str(label or "").strip().lower() == "api source"


def _translate_source_value(source_value: object, *, keep_suffix: bool = True) -> str:
    raw = str(source_value or "").strip()
    if not raw:
        return "—"

    mappings = {
        "live": t("source_live"),
        "fallback": t("source_fallback"),
        "estimate": t("source_estimate"),
    }
    raw_lower = raw.lower()

    for prefix, translated in mappings.items():
        if raw_lower.startswith(prefix):
            if keep_suffix and len(raw) > len(prefix):
                return f"{translated}{raw[len(prefix):]}"
            return translated

    return raw


def _resolve_customer_website_url(row: pd.Series) -> str:
    affiliate_url = _normalize_link_url(row.get("Affiliate", ""))
    if affiliate_url:
        return affiliate_url
    return _normalize_link_url(row.get("Website", ""))


def _get_market_links_for_row(row: pd.Series, symbol: str) -> list[dict[str, str]]:
    return get_market_data_source_links(
        str(row["Exchange"]),
        symbol=symbol,
        spread_source=str(row.get("Spread source", "")),
    )


def _build_exchange_links_html(row: pd.Series, symbol: str) -> str:
    link_html_parts: list[str] = []

    website_url = _resolve_customer_website_url(row)
    if website_url:
        link_html_parts.append(_build_link_html(t("website_label"), website_url))

    for item in _get_market_links_for_row(row, symbol):
        if _is_api_source_label(item.get("label")):
            continue
        item_url = _normalize_link_url(item.get("url"))
        if item_url:
            link_html_parts.append(
                _build_link_html(
                    _translate_link_label(item.get("label", "")),
                    item_url,
                )
            )

    fee_source_url = _normalize_link_url(row.get("Fee source", row.get("Source", "")))
    if fee_source_url:
        link_html_parts.append(_build_link_html(t("fee_source_col"), fee_source_url))

    if not link_html_parts:
        return ""

    return (
        '<div class="exchange-links">'
        + "".join(
            f'<span class="exchange-link">{link_html}</span>'
            for link_html in link_html_parts
        )
        + "</div>"
    )


def _get_reference_link_column(
    exchange_name: str,
    symbol: str,
    spread_source: str,
) -> str:
    links = get_market_data_source_links(
        exchange_name,
        symbol=symbol,
        spread_source=spread_source,
    )
    for item in links:
        if _is_api_source_label(item.get("label")):
            continue
        return _normalize_link_url(item.get("url"))
    return ""


def _source_badge(source: str) -> str:
    source = str(source or "").lower()
    if source.startswith("fallback"):
        return f'<span class="badge badge-fallback">{t("fallback_badge")}</span>'
    return f'<span class="badge badge-live">{t("live_badge")}</span>'


def _resolve_total_column(df: pd.DataFrame, amount: int) -> str:
    expected = f"Total € (op €{amount})"
    if expected in df.columns:
        return expected

    prefix_matches = [col for col in df.columns if str(col).startswith("Total € (op €")]
    if prefix_matches:
        return prefix_matches[0]

    generic_matches = [col for col in df.columns if str(col).startswith("Total €")]
    if generic_matches:
        return generic_matches[0]

    raise KeyError(f"No total-eur column found in dataframe columns: {list(df.columns)}")


def render_summary_cards(df: pd.DataFrame, amount: int) -> None:
    if df.empty:
        return

    total_col = _resolve_total_column(df, amount)
    ranked = df.sort_values(total_col, ascending=True).reset_index(drop=True)

    cheapest = ranked.iloc[0]
    best_spread = ranked.sort_values("Spread %", ascending=True).iloc[0]

    live_ts_values = [
        _extract_live_ts(v) for v in ranked["Spread source"].tolist() if _extract_live_ts(v)
    ]
    latest_ts = max(live_ts_values) if live_ts_values else ""

    cols = st.columns(4, gap="small")

    with cols[0]:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">{t("cheapest_exchange_label")}</div>
                <div class="metric-value">{cheapest["Exchange"]}</div>
                <div class="metric-subvalue">{t("cheapest_total_cost_summary", cost=_format_eur(cheapest[total_col]), amount=amount)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with cols[1]:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">{t("lowest_total_cost_label")}</div>
                <div class="metric-value">{_format_eur(cheapest[total_col])}</div>
                <div class="metric-subvalue">{t("based_on_amount", amount=amount)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with cols[2]:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">{t("best_spread_label")}</div>
                <div class="metric-value">{_format_spread_pct(float(best_spread["Spread %"]))}</div>
                <div class="metric-subvalue">{best_spread["Exchange"]}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with cols[3]:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">{t("latest_market_update_label")}</div>
                <div class="metric-value" style="font-size:1.18rem;">{_format_ts_short(latest_ts)}</div>
                <div class="metric-subvalue">{t("most_recent_visible_quote_label")}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_exchange_cards(df: pd.DataFrame, symbol: str, amount: int) -> None:
    if df.empty:
        st.info(t("no_comparison_data"))
        return

    st.markdown(
        f'<div class="section-title">{t("ranked_exchanges_title")}</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="section-subtitle">{t("ranked_exchanges_subtitle")}</div>',
        unsafe_allow_html=True,
    )

    total_col = _resolve_total_column(df, amount)
    ranked = df.sort_values(total_col, ascending=True).reset_index(drop=True)

    for i, (_, row) in enumerate(ranked.iterrows(), start=1):
        best_badge = f'<span class="badge badge-best">{t("best_price_badge")}</span>' if i == 1 else ""
        source_badge = _source_badge(row.get("Spread source", ""))
        source_text = _translate_source_value(row.get("Spread source", ""), keep_suffix=False)
        link_html = _build_exchange_links_html(row, symbol)

        st.markdown(
            f"""
            <div class="exchange-card">
                <div class="exchange-top">
                    <div>
                        <div class="exchange-rank">{t("rank_label", rank=i)}</div>
                        <div class="exchange-name">{row["Exchange"]}</div>
                        <div class="exchange-meta">{best_badge}{source_badge}</div>
                    </div>
                    <div class="exchange-total">
                        <div class="exchange-total-label">{t("total_cost_on_amount", amount=amount)}</div>
                        <div class="exchange-total-value">{_format_eur(row[total_col])}</div>
                    </div>
                </div>
                <div class="exchange-grid">
                    <div class="mini-stat">
                        <div class="mini-stat-label">{t("maker_fee_pct_short")}</div>
                        <div class="mini-stat-value">{_format_pct(float(row["Maker fee %"]))}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">{t("taker_fee_pct_short")}</div>
                        <div class="mini-stat-value">{_format_pct(float(row["Taker fee %"]))}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">{t("used_fee_pct_short")}</div>
                        <div class="mini-stat-value">{_format_pct(float(row["Used fee %"]))}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">{t("spread_pct_short")}</div>
                        <div class="mini-stat-value">{_format_spread_pct(float(row["Spread %"]))}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">{t("total_pct_short")}</div>
                        <div class="mini-stat-value">{_format_pct(float(row["Total %"]))}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">{t("quote_source_label")}</div>
                        <div class="mini-stat-value">{source_text}</div>
                    </div>
                </div>
                {link_html}
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_details_table(df: pd.DataFrame, symbol: str, amount: int) -> None:
    with st.expander(t("details_expander_label"), expanded=False):
        if df.empty:
            st.info(t("no_comparison_data"))
            st.download_button(
                t("export_csv_button"),
                data=df.to_csv(index=False).encode("utf-8"),
                file_name=f"crypto_fee_comparison_{symbol}_{amount}.csv",
                mime="text/csv",
                disabled=True,
            )
            return

        total_col = _resolve_total_column(df, amount)
        df_export = df.copy()
        reference_links = df_export.apply(
            lambda row: _get_reference_link_column(
                str(row["Exchange"]),
                symbol,
                str(row.get("Spread source", "")),
            ),
            axis=1,
        )
        df_export["Website"] = df_export.apply(_resolve_customer_website_url, axis=1)
        df_export["FX reference"] = reference_links
        df_export = df_export.drop(
            columns=["Affiliate", "API source", "iDEAL fee €", "EUR opname €"],
            errors="ignore",
        )
        if "FX reference" in df_export.columns and df_export["FX reference"].astype(str).str.strip().eq("").all():
            df_export = df_export.drop(columns=["FX reference"], errors="ignore")
        if "Type" in df_export.columns:
            df_export["Type"] = df_export["Type"].map(_translate_exchange_type)
        if "Spread source" in df_export.columns:
            df_export["Spread source"] = df_export["Spread source"].map(_translate_source_value)

        df_display = df_export.copy()
        df_display.insert(0, t("status_label"), "")

        cheapest_idx = df_display[total_col].idxmin()
        df_display.loc[cheapest_idx, t("status_label")] = t("status_cheapest")

        df_display = df_display.drop(columns=["Fee %", "Source"], errors="ignore")

        for col in ["Maker fee %", "Taker fee %", "Used fee %", "Total %"]:
            if col in df_display.columns:
                df_display[col] = df_display[col].map(_format_pct)

        if "Spread %" in df_display.columns:
            df_display["Spread %"] = df_display["Spread %"].map(_format_spread_pct)

        for col in [total_col]:
            if col in df_display.columns:
                df_display[col] = df_display[col].map(lambda v: f"€ {float(v):.2f}")

        translated_total_col = t("total_eur_col", amount=amount)
        column_labels = {
            "Exchange": t("exchange_col"),
            "Type": t("type_col"),
            "Maker fee %": t("maker_fee_pct_short"),
            "Taker fee %": t("taker_fee_pct_short"),
            "Used fee %": t("used_fee_pct_short"),
            "Spread %": t("spread_pct_short"),
            "Total %": t("total_pct_short"),
            "Spread source": t("quote_source_label"),
            "Website": t("website_label"),
            "FX reference": t("fx_reference_col"),
            "Fee source": t("fee_source_col"),
            "Fees bijgewerkt": t("fees_updated_col"),
            "Fees updated": t("fees_updated_col"),
            total_col: translated_total_col,
        }
        df_display = df_display.rename(columns=column_labels)
        df_export = df_export.rename(columns=column_labels)

        st.dataframe(df_display, width="stretch")
        st.download_button(
            t("export_csv_button"),
            data=df_export.to_csv(index=False).encode("utf-8"),
            file_name=f"crypto_fee_comparison_{symbol}_{amount}.csv",
            mime="text/csv",
        )


def render_costs_disclaimer() -> None:
    st.markdown(
        f"""
        <div class="kiraly-disclaimer">
            <div class="kiraly-disclaimer-text">{html.escape(t("costs_disclaimer"))}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_client_credit() -> None:
    credit_url = "https://www.instagram.com/studiocrypto.nl"
    credit_prefix = html.escape(t("studio_crypto_credit_prefix"))
    credit_label = html.escape(t("studio_crypto_credit_label"))
    credit_url_safe = html.escape(credit_url, quote=True)

    st.markdown(
        f"""
        <div class="client-credit">
            {credit_prefix}<a href="{credit_url_safe}" target="_blank" rel="noopener noreferrer">{credit_label}</a>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_debug(con, symbol: str) -> None:
    st.subheader(t("debug_title"))
    st.write(t("db_label"), str(DB_PATH))
    st.write(t("streamlit_cloud_mode_label"), is_streamlit_cloud())
    st.write(t("live_exchanges_label"), list(LIVE_EXCHANGES))

    exchanges = list_exchanges(con)
    exchange_names = [str(row["name"]) for row in exchanges]
    st.write(t("exchanges_label"), exchange_names)

    cur = con.cursor()
    cur.execute(
        """
        SELECT e.name AS exchange,
               COUNT(q.rowid) AS quote_count,
               MAX(q.ts) AS latest_ts
        FROM exchanges e
        LEFT JOIN quotes q
               ON q.exchange_id = e.id
              AND q.symbol = ?
        GROUP BY e.id, e.name
        ORDER BY e.name
        """,
        (symbol,),
    )
    rows = cur.fetchall()
    st.dataframe(
        pd.DataFrame(
            [
                {
                    t("exchange_col"): str(row["exchange"]),
                    t("quote_count_col"): int(row["quote_count"]),
                    t("latest_ts_col"): row["latest_ts"],
                }
                for row in rows
            ]
        ),
        width="stretch",
    )


def render_site_footer() -> None:
    footer_url = "https://kiralyai.com"
    footer_primary = html.escape(t("footer_primary"))
    footer_secondary_prefix = html.escape(t("footer_secondary_prefix"))
    footer_secondary_link_label = html.escape(t("footer_secondary_link_label"))
    footer_secondary_suffix = html.escape(t("footer_secondary_suffix"))
    footer_url_safe = html.escape(footer_url, quote=True)

    st.markdown(
        f"""
        <div class="site-footer">
            <div class="site-footer-primary">{footer_primary}</div>
            <div class="site-footer-secondary">
                {footer_secondary_prefix}<a href="{footer_url_safe}" target="_blank" rel="noopener noreferrer">{footer_secondary_link_label}</a>{footer_secondary_suffix}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


apply_light_style()
render_header()

ADMIN_MODE = _is_admin_mode()

init_db()
con = connect()
ensure_seed_data(con)

symbol, amount = render_controls(con)

dashboard_exchanges = _get_dashboard_exchanges(con)

_, refresh_failures, fallback_used = _refresh_live_quotes(
    con, symbol=symbol, exchange_names=dashboard_exchanges
)

if "fallback_exchanges" not in st.session_state:
    st.session_state["fallback_exchanges"] = []

if fallback_used:
    st.session_state["fallback_exchanges"] = fallback_used

if refresh_failures:
    refresh_failures = {name: err for name, err in refresh_failures.items() if name in LIVE_EXCHANGES}
    if refresh_failures and ADMIN_MODE:
        st.warning(
            t(
                "live_quote_refresh_issues",
                details="; ".join(
                    [
                        f"{name}: {_translate_service_error_message(error)}"
                        for name, error in refresh_failures.items()
                    ]
                ),
            )
        )

try:
    df = build_comparison_dataframe(con, symbol=symbol, amount=float(amount))
except ServiceError as exc:
    st.error(_translate_service_error_message(exc))
    df = pd.DataFrame()
except Exception as exc:
    st.error(t("unexpected_dashboard_error", error=exc))
    df = pd.DataFrame()

if not df.empty:
    df = df[df["Exchange"].isin(dashboard_exchanges)].copy()

    if "Spread source" in df.columns:
        fallback_set = set(st.session_state.get("fallback_exchanges", []))
        if fallback_set:
            df.loc[df["Exchange"].isin(fallback_set), "Spread source"] = "fallback (BTCUSDT * USDT->EUR)"

render_summary_cards(df, amount=int(amount))
st.markdown("")
render_exchange_cards(df, symbol=symbol, amount=int(amount))
render_details_table(df, symbol=symbol, amount=int(amount))
if not df.empty:
    render_costs_disclaimer()
    render_client_credit()

if ADMIN_MODE:
    st.divider()
    render_debug(con, symbol=symbol)
    st.divider()
    render_admin(con)

st.caption(
    t("admin_tip_caption")
    if ADMIN_MODE
    else ""
)
render_site_footer()

con.close()
