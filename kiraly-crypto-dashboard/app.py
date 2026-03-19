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

LIVE_EXCHANGES = ("Bitvavo", "Kraken", "Coinbase", "Bybit", "OKX")

st.set_page_config(
    page_title="KiralyAI | Crypto Exchange Cost Dashboard",
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
        left_col, right_col = st.columns([1, 8], vertical_alignment="center")

        with left_col:
            try:
                st.image(str(Path("assets/kiraly-logo.png")), width=88)
            except Exception:
                pass

        with right_col:
            st.markdown('<div class="hero-wrap">', unsafe_allow_html=True)
            st.markdown('<div class="hero-chip">Live market overview</div>', unsafe_allow_html=True)
            st.markdown('<div class="hero-title">Crypto Exchange Cost Dashboard</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="hero-subtitle">Compare the total EUR cost of buying BTC across major exchanges. See the cheapest venue instantly, with live spread and fee impact.</div>',
                unsafe_allow_html=True,
            )
            st.markdown("</div>", unsafe_allow_html=True)


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
            st.markdown('<div class="control-label">Pair</div>', unsafe_allow_html=True)
            symbol = st.selectbox("Pair", ["BTC-EUR"], label_visibility="collapsed")

        with c2:
            st.markdown('<div class="control-label">Investment amount</div>', unsafe_allow_html=True)
            amount = st.selectbox("Amount", [100, 1000, 10000], index=1, label_visibility="collapsed")

        with c3:
            st.markdown('<div class="control-label">Market data</div>', unsafe_allow_html=True)
            exchange_names_for_fetch = _get_dashboard_exchanges(con)
            fetch_clicked = st.button(
                "Refresh Market Data",
                disabled=not exchange_names_for_fetch,
                use_container_width=True,
            )

        if fetch_clicked:
            refreshed, failed, fallback_used = _refresh_live_quotes(
                con, symbol=symbol, exchange_names=exchange_names_for_fetch
            )
            st.session_state["fallback_exchanges"] = fallback_used

            if refreshed:
                st.success(f"Updated market quotes for: {', '.join(refreshed)}")

            if failed:
                failed = {name: err for name, err in failed.items() if name in LIVE_EXCHANGES}
                if failed and ADMIN_MODE:
                    st.warning(
                        "Some exchanges failed: "
                        + "; ".join([f"{name}: {error}" for name, error in failed.items()])
                    )

    return symbol, amount


def render_admin(con) -> None:
    st.subheader("Admin: fees and links")

    with st.expander("Open fee editor", expanded=False):
        with st.container(border=True):
            st.markdown("#### Add exchange (admin)")
            with st.form("add_exchange_form", clear_on_submit=True):
                new_exchange_name = st.text_input("Name", key="new_exchange_name")
                new_exchange_type = st.selectbox("Type", ["exchange", "broker"], key="new_exchange_type")
                new_exchange_website = st.text_input("Website", key="new_exchange_website")
                new_exchange_affiliate_url = st.text_input(
                    "Affiliate URL",
                    key="new_exchange_affiliate_url",
                )
                add_exchange_submitted = st.form_submit_button("Add exchange")

            if add_exchange_submitted:
                try:
                    add_exchange_with_defaults(
                        con,
                        name=new_exchange_name,
                        exchange_type=new_exchange_type,
                        website=new_exchange_website,
                        affiliate_url=new_exchange_affiliate_url,
                    )
                    st.success(f"Added exchange: {new_exchange_name.strip()}")
                    st.rerun()
                except ServiceError as exc:
                    st.error(str(exc))

            try:
                exchanges = list_exchanges(con)
            except Exception as exc:
                exchanges = []
                st.error(f"Could not load exchanges: {exc}")

            if not exchanges:
                st.info("No exchanges found. Add one above.")
                return

            ex_name_to_id = {str(ex["name"]): int(ex["id"]) for ex in exchanges}
            selected_name = st.selectbox("Exchange", list(ex_name_to_id.keys()))
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

            st.markdown("#### Edit exchange")
            profile_col_a, profile_col_b = st.columns(2)
            with profile_col_a:
                new_website = st.text_input(
                    "Website",
                    value=str(selected_exchange["website"] or ""),
                )
            with profile_col_b:
                new_affiliate_url = st.text_input(
                    "Affiliate URL",
                    value=str(selected_exchange["affiliate_url"] or ""),
                )

            col_a, col_b = st.columns(2)
            with col_a:
                new_maker_fee = st.number_input(
                    "Maker fee %",
                    value=maker_fee_pct,
                    step=0.01,
                    format="%.4f",
                    help="Shown for transparency, but not used in total-cost ranking.",
                )
                new_taker_fee = st.number_input(
                    "Taker fee %",
                    value=taker_fee_pct,
                    step=0.01,
                    format="%.4f",
                    help="Used for total-cost ranking and mirrored into trading_fee_pct.",
                )
                new_spread_est = st.number_input(
                    "Spread estimate %",
                    value=spread_estimate_pct,
                    step=0.01,
                    format="%.4f",
                    help="Fallback spread als er geen live orderbook quote is.",
                )
            with col_b:
                new_ideal_fee = st.number_input(
                    "iDEAL deposit fee (€)",
                    value=deposit_ideal_fee_eur,
                    step=0.10,
                    format="%.2f",
                )
                new_withdraw_fee = st.number_input(
                    "EUR withdrawal fee (€)",
                    value=withdraw_eur_fee_eur,
                    step=0.10,
                    format="%.2f",
                )

            new_source = st.text_input("Fee source URL", value=source_url)

            if st.button("Save exchange settings"):
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
                        deposit_ideal_fee_eur=float(new_ideal_fee),
                        withdraw_eur_fee_eur=float(new_withdraw_fee),
                        spread_estimate_pct=float(new_spread_est),
                        source_url=new_source,
                        maker_fee_pct=float(new_maker_fee),
                        taker_fee_pct=float(new_taker_fee),
                    )
                    st.success(f"Saved exchange settings for {selected_name}")
                    st.rerun()
                except ServiceError as exc:
                    st.error(str(exc))

            st.markdown("#### Delete exchange")
            st.markdown(
                '<div class="kiraly-subtle">Only remove exchanges you no longer want to compare.</div>',
                unsafe_allow_html=True,
            )
            with st.form("delete_exchange_form"):
                delete_exchange_name = st.selectbox(
                    "Exchange to delete",
                    list(ex_name_to_id.keys()),
                    key="delete_exchange_name",
                )
                confirm_delete = st.checkbox(
                    "I understand this will delete related fees and quotes.",
                    key="confirm_delete_exchange",
                )
                delete_exchange_submitted = st.form_submit_button("Delete exchange")

            if delete_exchange_submitted:
                if not confirm_delete:
                    st.warning("Please confirm delete before continuing.")
                else:
                    try:
                        delete_exchange_cascade(con, ex_name_to_id[delete_exchange_name])
                        st.success(f"Deleted exchange: {delete_exchange_name}")
                        st.rerun()
                    except ServiceError as exc:
                        st.error(str(exc))


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
        link_html_parts.append(_build_link_html("Website", website_url))

    for item in _get_market_links_for_row(row, symbol):
        item_url = _normalize_link_url(item.get("url"))
        if item_url:
            link_html_parts.append(_build_link_html(item.get("label", "API source"), item_url))

    fee_source_url = _normalize_link_url(row.get("Fee source", row.get("Source", "")))
    if fee_source_url:
        link_html_parts.append(_build_link_html("Fee source", fee_source_url))

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


def _get_market_link_columns(
    exchange_name: str,
    symbol: str,
    spread_source: str,
) -> tuple[str, str]:
    links = get_market_data_source_links(
        exchange_name,
        symbol=symbol,
        spread_source=spread_source,
    )
    market_api = _normalize_link_url(links[0]["url"]) if links else ""
    reference_api = _normalize_link_url(links[1]["url"]) if len(links) > 1 else ""
    return market_api, reference_api


def _source_badge(source: str) -> str:
    source = str(source or "").lower()
    if source.startswith("fallback"):
        return '<span class="badge badge-fallback">Fallback</span>'
    return '<span class="badge badge-live">Live</span>'


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
                <div class="metric-label">Cheapest exchange</div>
                <div class="metric-value">{cheapest["Exchange"]}</div>
                <div class="metric-subvalue">{_format_eur(cheapest[total_col])} total cost</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with cols[1]:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Lowest total cost</div>
                <div class="metric-value">{_format_eur(cheapest[total_col])}</div>
                <div class="metric-subvalue">Based on € {amount}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with cols[2]:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Best spread</div>
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
                <div class="metric-label">Latest market update</div>
                <div class="metric-value" style="font-size:1.18rem;">{_format_ts_short(latest_ts)}</div>
                <div class="metric-subvalue">Most recent visible quote</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_exchange_cards(df: pd.DataFrame, symbol: str, amount: int) -> None:
    if df.empty:
        st.info("No comparison data available.")
        return

    st.markdown('<div class="section-title">Ranked exchanges</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-subtitle">A cleaner view of the total BTC purchase cost across all supported venues.</div>',
        unsafe_allow_html=True,
    )

    total_col = _resolve_total_column(df, amount)
    ranked = df.sort_values(total_col, ascending=True).reset_index(drop=True)

    for i, (_, row) in enumerate(ranked.iterrows(), start=1):
        best_badge = '<span class="badge badge-best">Best price</span>' if i == 1 else ""
        source_badge = _source_badge(row.get("Spread source", ""))
        source_text = str(row.get("Spread source", "")).split("(")[0].strip().title() or "—"
        link_html = _build_exchange_links_html(row, symbol)

        st.markdown(
            f"""
            <div class="exchange-card">
                <div class="exchange-top">
                    <div>
                        <div class="exchange-rank">Rank #{i}</div>
                        <div class="exchange-name">{row["Exchange"]}</div>
                        <div class="exchange-meta">{best_badge}{source_badge}</div>
                    </div>
                    <div class="exchange-total">
                        <div class="exchange-total-label">Total cost on € {amount}</div>
                        <div class="exchange-total-value">{_format_eur(row[total_col])}</div>
                    </div>
                </div>
                <div class="exchange-grid">
                    <div class="mini-stat">
                        <div class="mini-stat-label">Maker fee %</div>
                        <div class="mini-stat-value">{_format_pct(float(row["Maker fee %"]))}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">Taker fee %</div>
                        <div class="mini-stat-value">{_format_pct(float(row["Taker fee %"]))}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">Used fee %</div>
                        <div class="mini-stat-value">{_format_pct(float(row["Used fee %"]))}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">Spread %</div>
                        <div class="mini-stat-value">{_format_spread_pct(float(row["Spread %"]))}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">Total %</div>
                        <div class="mini-stat-value">{_format_pct(float(row["Total %"]))}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">Quote source</div>
                        <div class="mini-stat-value">{source_text}</div>
                    </div>
                </div>
                {link_html}
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_details_table(df: pd.DataFrame, symbol: str, amount: int) -> None:
    with st.expander("Open detailed comparison data", expanded=False):
        if df.empty:
            st.info("No comparison data available.")
            st.download_button(
                "Export CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name=f"crypto_fee_comparison_{symbol}_{amount}.csv",
                mime="text/csv",
                disabled=True,
            )
            return

        total_col = _resolve_total_column(df, amount)
        df_export = df.copy()
        market_links = df_export.apply(
            lambda row: _get_market_link_columns(
                str(row["Exchange"]),
                symbol,
                str(row.get("Spread source", "")),
            ),
            axis=1,
        )
        df_export["Website"] = df_export.apply(_resolve_customer_website_url, axis=1)
        df_export["API source"] = market_links.map(lambda links: links[0])
        df_export["FX reference"] = market_links.map(lambda links: links[1])
        df_export = df_export.drop(columns=["Affiliate"], errors="ignore")

        df_display = df_export.copy()
        df_display.insert(0, "Status", "")

        cheapest_idx = df_display[total_col].idxmin()
        df_display.loc[cheapest_idx, "Status"] = "Cheapest"

        df_display = df_display.drop(columns=["Fee %", "Source"], errors="ignore")

        for col in ["Maker fee %", "Taker fee %", "Used fee %", "Total %"]:
            if col in df_display.columns:
                df_display[col] = df_display[col].map(_format_pct)

        if "Spread %" in df_display.columns:
            df_display["Spread %"] = df_display["Spread %"].map(_format_spread_pct)

        for col in [total_col, "iDEAL fee €", "EUR opname €"]:
            if col in df_display.columns:
                df_display[col] = df_display[col].map(lambda v: f"€ {float(v):.2f}")

        st.dataframe(df_display, width="stretch")
        st.download_button(
            "Export CSV",
            data=df_export.to_csv(index=False).encode("utf-8"),
            file_name=f"crypto_fee_comparison_{symbol}_{amount}.csv",
            mime="text/csv",
        )


def render_debug(con, symbol: str) -> None:
    st.subheader("Debug")
    st.write("DB:", str(DB_PATH))
    st.write("Streamlit Cloud mode:", is_streamlit_cloud())
    st.write("Live exchanges:", list(LIVE_EXCHANGES))

    exchanges = list_exchanges(con)
    exchange_names = [str(row["name"]) for row in exchanges]
    st.write("Exchanges:", exchange_names)

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
                    "Exchange": str(row["exchange"]),
                    "Quote count": int(row["quote_count"]),
                    "Latest ts": row["latest_ts"],
                }
                for row in rows
            ]
        ),
        width="stretch",
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
            "Live quote refresh issues: "
            + "; ".join([f"{name}: {error}" for name, error in refresh_failures.items()])
        )

try:
    df = build_comparison_dataframe(con, symbol=symbol, amount=float(amount))
except ServiceError as exc:
    st.error(str(exc))
    df = pd.DataFrame()
except Exception as exc:
    st.error(f"Unexpected error while building dashboard: {exc}")
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

if ADMIN_MODE:
    st.divider()
    render_debug(con, symbol=symbol)
    st.divider()
    render_admin(con)

st.caption(
    "Tip: total-cost ranking uses taker fees from the database, while spreads and market/API sources are refreshed from live data."
    if ADMIN_MODE
    else ""
)

con.close()
