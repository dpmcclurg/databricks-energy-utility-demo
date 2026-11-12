"""Winter Volume Plan: Colorado Gas Utility (fictional) FP&A app on Databricks Apps.

Tab 1, Winter Volume Plan: the season forecast by region, regional sign-off, publish to the trading
desk, and the design day against firm transportation. Tab 2, Season Re-forecast: actual to date plus
the NWS 7-day forecast plus historical winters for the rest, variance to date in three parts (weather,
customers, other), and the Genie Agent for in-season questions.

Reads come from Lakebase (synced from Unity Catalog). Sign-offs and published versions are written
to Lakebase; each publish is also appended to the governed Unity Catalog table the trading desk reads.
"""
import os
import time
from contextlib import contextmanager

import altair as alt
import numpy as np
import pandas as pd
import psycopg
import streamlit as st
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

st.set_page_config(page_title="Winter Volume Plan", layout="wide")

CATALOG = os.environ["UC_CATALOG"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "none")
LAKEBASE_ENDPOINT = os.environ["LAKEBASE_ENDPOINT"]
SERVING = "cgu_serving"
APP = os.environ.get("APP_SCHEMA", "app")  # Postgres schema owned by the app; local dev uses its own
REGIONS = ["summit", "teller", "park_foothills", "pueblo", "eastern_plains"]
REGION_NAMES = {"summit": "Summit", "teller": "Teller", "park_foothills": "Park and Foothills",
                "pueblo": "Pueblo", "eastern_plains": "Eastern Plains"}

cfg = Config()
w = WorkspaceClient()


# Lakebase -------------------------------------------------------------------------------------

@st.cache_resource(ttl=2700)  # OAuth database credentials last an hour; refresh before then
def pg_password() -> str:
    return w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT).token


@contextmanager
def pg():
    conn = psycopg.connect(host=os.environ["PGHOST"], port=os.environ.get("PGPORT", "5432"),
                           dbname=os.environ["PGDATABASE"], user=os.environ["PGUSER"],
                           password=pg_password(), sslmode="require", autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


def pg_df(query: str, params=None) -> pd.DataFrame:
    with pg() as conn, conn.cursor() as cur:
        cur.execute(query, params)
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


@st.cache_resource
def ensure_app_tables() -> bool:
    with pg() as conn:
        if not conn.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (APP,)).fetchone():
            conn.execute(f"CREATE SCHEMA {APP}")  # the app's service principal becomes the owner
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {APP}.plan_signoff (
            season TEXT, region_id TEXT, forecast_version TEXT, status TEXT, signed_by TEXT, comment TEXT,
            signed_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY (season, region_id, forecast_version))""")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {APP}.plan_published (
            plan_version INT, season TEXT, region_id TEXT, period TEXT, p10 DOUBLE PRECISION,
            p50 DOUBLE PRECISION, p90 DOUBLE PRECISION, model_version TEXT, published_by TEXT,
            published_at TIMESTAMPTZ, PRIMARY KEY (season, plan_version, region_id, period))""")
        pk = {r[0] for r in conn.execute(
            "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
            "WHERE i.indrelid = %s::regclass AND i.indisprimary", (f"{APP}.plan_published",)).fetchall()}
        if "season" not in pk:  # versions are numbered per season
            conn.execute(f"ALTER TABLE {APP}.plan_published DROP CONSTRAINT plan_published_pkey, "
                         "ADD PRIMARY KEY (season, plan_version, region_id, period)")
    return True


def current_user() -> str:
    return st.context.headers.get("X-Forwarded-Email") or st.context.headers.get("X-Forwarded-User") or "local-dev"


# Unity Catalog hand-off -----------------------------------------------------------------------

def publish_to_uc(rows: pd.DataFrame) -> None:
    """Append the published version to cgu_fpa.plan_published, the table the trading desk opens in Excel."""
    with dbsql.connect(server_hostname=cfg.host, http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
                       credentials_provider=lambda: cfg.authenticate) as conn, conn.cursor() as cur:
        values = ", ".join(
            f"({int(r.plan_version)}, '{r.season}', '{r.region_id}', '{r.period}', {r.p10}, {r.p50}, {r.p90}, "
            f"'{r.model_version}', '{r.published_by}', TIMESTAMP'{r.published_at:%Y-%m-%d %H:%M:%S}')"
            for r in rows.itertuples())
        cur.execute(f"INSERT INTO `{CATALOG}`.cgu_fpa.plan_published VALUES {values}")


# Data -----------------------------------------------------------------------------------------

@st.cache_data(ttl=60)
def load_plan():
    fc = pg_df(f"SELECT * FROM {SERVING}.forecast_season")
    dq = pg_df(f"SELECT * FROM {SERVING}.gold_dq_summary")
    rec = pg_df(f"SELECT * FROM {SERVING}.gold_reconciliation")
    return fc, dq, rec


@st.cache_data(ttl=60)
def load_design_day():
    return pg_df(f"SELECT * FROM {SERVING}.design_day")


@st.cache_data(ttl=60)
def load_tracker():
    return (pg_df(f"SELECT * FROM {SERVING}.season_tracker_summary"),
            pg_df(f"SELECT * FROM {SERVING}.season_tracker ORDER BY region_id, gas_day"),
            pg_df(f"SELECT * FROM {SERVING}.season_reforecast ORDER BY region_id, batch"))


def signoffs(season: str, version: str) -> pd.DataFrame:
    return pg_df(f"SELECT region_id, status, signed_by, signed_at FROM {APP}.plan_signoff "
                 "WHERE season = %s AND forecast_version = %s", (season, version))


def published_versions(season: str) -> pd.DataFrame:
    return pg_df("SELECT plan_version, MAX(published_by) published_by, MAX(published_at) published_at, "
                 "MAX(model_version) model_version, SUM(p50) FILTER (WHERE period = 'Nov-Mar') season_p50 "
                 f"FROM {APP}.plan_published WHERE season = %s GROUP BY plan_version ORDER BY plan_version DESC", (season,))


def fmt(x) -> str:
    return f"{x:,.0f}"


def pct(x) -> str:
    return f"{x:+.1f}%"


# Layout ---------------------------------------------------------------------------------------

ensure_app_tables()
fc, dq, rec = load_plan()
SEASON = str(fc.season.iloc[0])
st.title("Colorado Gas Utility: Winter Volume Plan")
st.caption(f"Winter {SEASON}. Fictional utility: synthetic volumes and customers, driven by real NOAA weather and the real "
           "NWS forecast. Dth = dekatherms (1 Dth = 1 MMBtu).")

plan_tab, tracker_tab = st.tabs(["Winter Volume Plan", "Season Re-forecast"])

with plan_tab:
    version = str(fc.model_version.iloc[0])
    season = fc[fc.period_type == "season"].set_index("region_id")
    monthly = fc[fc.period_type == "month"].copy()
    versions = published_versions(SEASON)
    signed = signoffs(SEASON, version)
    signed_regions = set(signed[signed.status == "signed"].region_id)

    total_p50 = season.p50.sum()
    latest = None if versions.empty else versions.iloc[0]
    is_current = latest is not None and str(latest.model_version) == version

    vol_dq = dq[dq.target == "daily_volume"]
    checked = int(vol_dq.groupby("batch").rows_checked.max().sum())
    quarantined = int(vol_dq[vol_dq.action == "drop"].rows_failed.sum())
    flagged = int(vol_dq[vol_dq.action == "warn"].rows_failed.sum())
    balanced = "all extracts balanced" if (rec.status == "BALANCED").all() else "OUT OF BALANCE"

    k = st.columns(5)
    k[0].metric("Season requirement (P50)", f"{fmt(total_p50)} Dth")
    k[0].caption("Expected winter, Nov to Mar")
    k[1].metric("Cold case (P90)", f"{fmt(season.p90.sum())} Dth")
    k[1].caption(f"{fmt(season.p90.sum() - total_p50)} Dth above P50")
    if latest is None:
        k[2].metric("Trading desk has", "Nothing yet")
        k[2].caption("Publish once all regions sign off")
    else:
        gap = total_p50 - float(latest.season_p50)
        k[2].metric("Trading desk has", f"v{int(latest.plan_version)}")
        k[2].caption("Current plan" if is_current and abs(gap) < 0.5 else f"{fmt(gap)} Dth behind the current forecast")
    k[3].metric("Records checked", fmt(checked))
    k[3].caption(f"{quarantined} quarantined, {flagged} estimated reads flagged")
    k[4].metric("Forecast date", str(fc.forecast_date.iloc[0]))
    k[4].caption(f"Model v{version}, {balanced}")

    st.subheader("Regions")
    cols = st.columns(len(REGIONS))
    for col, region in zip(cols, REGIONS):
        s = season.loc[region]
        with col.container(border=True):
            st.markdown(f"**{REGION_NAMES[region]}**")
            st.markdown(f"P50 **{fmt(s.p50)}** Dth  \nP10 {fmt(s.p10)} / P90 {fmt(s.p90)}  \n{fmt(s.customers_planned)} customers planned")
            m = monthly[monthly.region_id == region].sort_values("period")
            band = alt.Chart(m).mark_area(opacity=0.25).encode(
                x=alt.X("period:N", title=None, axis=alt.Axis(labelAngle=0, labelExpr="slice(datum.value, 5)")),
                y=alt.Y("p10:Q", title=None, axis=alt.Axis(format="~s")), y2="p90:Q")
            line = alt.Chart(m).mark_line(point=True).encode(x="period:N", y="p50:Q")
            st.altair_chart((band + line).properties(height=140), width="stretch")
            if region in signed_regions:
                who = signed[signed.region_id == region].iloc[0]
                st.success(f"Signed by {who.signed_by.split('@')[0]}")
            elif st.button("Sign off", key=f"sign_{region}", width="stretch"):
                with pg() as conn:
                    conn.execute(f"INSERT INTO {APP}.plan_signoff (season, region_id, forecast_version, status, signed_by) "
                                 "VALUES (%s, %s, %s, 'signed', %s) ON CONFLICT (season, region_id, forecast_version) "
                                 "DO UPDATE SET status = 'signed', signed_by = EXCLUDED.signed_by, signed_at = now()",
                                 (SEASON, region, version, current_user()))
                print(f"SIGNOFF season={SEASON} region={region} forecast_version={version}", flush=True)
                st.rerun()

    ready = signed_regions >= set(REGIONS)
    left, right = st.columns([1, 3])
    with left:
        if st.button("Publish to trading desk", type="primary", disabled=not ready or is_current, width="stretch"):
            next_version = 1 if versions.empty else int(versions.plan_version.max()) + 1
            rows = fc[["season", "region_id", "period", "p10", "p50", "p90", "model_version"]].assign(
                plan_version=next_version, published_by=current_user(), published_at=pd.Timestamp.utcnow().tz_localize(None))
            with pg() as conn, conn.cursor() as cur:
                cur.executemany(
                    f"INSERT INTO {APP}.plan_published (plan_version, season, region_id, period, p10, p50, p90, model_version, "
                    "published_by, published_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    [(int(r.plan_version), r.season, r.region_id, r.period, float(r.p10), float(r.p50), float(r.p90),
                      str(r.model_version), r.published_by, r.published_at) for r in rows.itertuples()])
            publish_to_uc(rows)
            print(f"PUBLISH plan_version={next_version} season={SEASON} rows={len(rows)} season_p50={total_p50:,.0f} "
                  f"lakebase=app.plan_published uc=cgu_fpa.plan_published", flush=True)
            st.toast(f"Published version {next_version} to the trading desk")
            time.sleep(1)
            st.rerun()
        if is_current:
            st.caption(f"This forecast is published as v{int(latest.plan_version)}. A new forecast reopens sign-off.")
        elif not ready:
            st.caption(f"{len(signed_regions)} of {len(REGIONS)} regions signed. Publish unlocks when all regions sign off.")
    with right:
        st.markdown("**Published versions**")
        if versions.empty:
            st.caption("Nothing published yet.")
        else:
            st.dataframe(
                versions.assign(published_by=versions.published_by.str.split("@").str[0]), hide_index=True, width="stretch",
                column_config={"plan_version": st.column_config.NumberColumn("Version", format="v%d"),
                               "published_by": "Published by",
                               "published_at": st.column_config.DatetimeColumn("Published at", format="YYYY-MM-DD HH:mm"),
                               "model_version": "Model version",
                               "season_p50": st.column_config.NumberColumn("Season P50 (Dth)", format="%,.0f")})

    st.subheader("Peak day and firm transportation")
    dd = load_design_day().set_index("region_id").loc[REGIONS].reset_index()
    short = dd[dd.status == "short"]
    st.caption("Season HDD sizes how much gas to buy; the design day sizes how much firm pipeline capacity to hold. "
               f"Design day = the 1-in-30 coldest day from a GEV fit of {int(dd.winters_used.min())}+ winters of station data, "
               f"with today's customers (as of {dd.as_of_date.max()}). Firm transportation contracts are fictional.")
    view = pd.DataFrame({
        "Region": dd.region_id.map(REGION_NAMES),
        "Design HDD (1-in-30)": dd.design_hdd_1in30,
        "Peak day, 1-in-30 (Dth)": dd.peak_dth_1in30,
        "90% band (Dth)": [f"{fmt(a)} to {fmt(b)}" for a, b in zip(dd.peak_dth_1in30_low, dd.peak_dth_1in30_high)],
        "Peak day, 1-in-50 (Dth)": dd.peak_dth_1in50,
        "Firm capacity (Dth/day)": dd.contracted_dth_per_day,
        "Headroom": dd.headroom_pct,
        "Status": dd.status.map({"short": "Short", "covered": "Covered"}),
    })
    left, right = st.columns([3, 2])
    left.dataframe(view, hide_index=True, width="stretch", column_config={
        "Design HDD (1-in-30)": st.column_config.NumberColumn(format="%.1f"),
        **{c: st.column_config.NumberColumn(format="%,.0f") for c in
           ["Peak day, 1-in-30 (Dth)", "Peak day, 1-in-50 (Dth)", "Firm capacity (Dth/day)"]},
        "Headroom": st.column_config.NumberColumn(format="%+.1f%%")})
    bars = dd.assign(Region=dd.region_id.map(REGION_NAMES))
    peak = alt.Chart(bars).mark_bar(color="#1A73E8", opacity=0.85).encode(
        y=alt.Y("Region:N", title=None, sort=[REGION_NAMES[r] for r in REGIONS]),
        x=alt.X("peak_dth_1in30:Q", title="Dth/day", axis=alt.Axis(format="~s")))
    whisk = alt.Chart(bars).mark_rule(color="#174EA6").encode(
        y=alt.Y("Region:N", sort=[REGION_NAMES[r] for r in REGIONS]), x="peak_dth_1in30_low:Q", x2="peak_dth_1in30_high:Q")
    cap = alt.Chart(bars).mark_tick(color="#202124", thickness=3, size=26).encode(
        y=alt.Y("Region:N", sort=[REGION_NAMES[r] for r in REGIONS]), x="contracted_dth_per_day:Q")
    right.markdown("**1-in-30 peak day (bar, with 90% band) against firm capacity (black tick)**")
    right.altair_chart((peak + whisk + cap).properties(height=220), width="stretch")
    for r in short.itertuples():
        st.warning(f"{REGION_NAMES[r.region_id]} is short {fmt(-r.headroom_dth)} Dth/day ({r.headroom_pct:+.1f}%) on a 1-in-30 day "
                   f"with {fmt(r.customers)} customers against {fmt(r.contracted_dth_per_day)} Dth/day contracted. "
                   "Cover with added firm capacity, peak-shaving, or a storage call.")

with tracker_tab:
    summary, daily, history = load_tracker()
    summary = summary.set_index("region_id").loc[REGIONS].reset_index()
    as_of = summary.as_of_date.max()
    days_actual = int(summary.days_actual.max())
    issued = summary.nws_issued_at.dropna().max() if summary.nws_issued_at.notna().any() else None
    refreshed = pd.Timestamp(summary.refreshed_at.max())
    st.subheader(f"Season re-forecast, as of {as_of}")
    early = " Early signal: with only the first weeks of actuals, percentages move a lot from run to run." if 0 < days_actual < 21 else ""
    actuals = (f"{days_actual} days of actuals (November 1 to {as_of})." if days_actual else
               f"No November actuals yet: weather stations have reported through {as_of}. The season so far is forecast.")
    st.caption(f"Refreshed {refreshed:%b %d, %H:%M} UTC. {actuals} "
               + (f"NWS forecast issued {str(issued)[:16].replace('T', ' ')} UTC. " if issued else "No NWS forecast stored. ")
               + "Rest of season: each historical winter, with today's customers." + early)

    re50, pub50 = summary.reforecast_p50.sum(), summary.published_p50.sum()
    k = st.columns(4)
    k[0].metric("Season re-forecast (P50)", f"{fmt(re50)} Dth")
    k[0].caption(f"{pct(100 * (re50 / pub50 - 1))} vs the published plan ({fmt(re50 - pub50)} Dth)")
    k[1].metric("Re-forecast range (P10 to P90)", f"{fmt(summary.reforecast_p10.sum() / 1e3)}k to {fmt(summary.reforecast_p90.sum() / 1e3)}k")
    k[1].caption(f"Published plan: {fmt(summary.published_p10.sum() / 1e3)}k to {fmt(summary.published_p90.sum() / 1e3)}k Dth")
    nh, nn = summary.next7_hdd.mean(), summary.next7_normal_hdd.mean()
    k[2].metric("Next 7 days, HDD (region average)", f"{nh:,.0f}")
    k[2].caption(f"Normal {nn:,.0f}: {'colder' if nh > nn else 'warmer'} than normal by {abs(nh - nn):,.0f} HDD" if nn else "No forecast days in season")
    k[3].metric("Days of actuals", f"{days_actual}")
    k[3].caption(f"{int(summary.estimated_days.sum())} region-days on estimated reads")

    st.markdown("**Season range by region: published plan against the latest re-forecast (Dth)**")
    rng = pd.concat([
        summary.assign(which="Published plan", p10=summary.published_p10, p50=summary.published_p50, p90=summary.published_p90),
        summary.assign(which=f"Re-forecast as of {as_of}", p10=summary.reforecast_p10, p50=summary.reforecast_p50, p90=summary.reforecast_p90),
    ])
    rng["Region"] = rng.region_id.map(REGION_NAMES)
    enc = dict(y=alt.Y("Region:N", title=None, sort=[REGION_NAMES[r] for r in REGIONS]),
               yOffset=alt.YOffset("which:N", sort=["Published plan"]),
               color=alt.Color("which:N", title=None, scale=alt.Scale(range=["#9AA0A6", "#1A73E8"]), legend=alt.Legend(orient="top")))
    st.altair_chart((alt.Chart(rng).mark_bar(size=10, opacity=0.8).encode(x=alt.X("p10:Q", title=None, axis=alt.Axis(format="~s")), x2="p90:Q", **enc)
                     + alt.Chart(rng).mark_tick(thickness=2, size=14, color="#202124").encode(x="p50:Q", **enc)).properties(height=260),
                    width="stretch")

    view = pd.DataFrame({
        "Region": summary.region_id.map(REGION_NAMES),
        "Plan to date (Dth)": summary.plan_to_date, "Actual to date (Dth)": summary.actual_to_date,
        "Weather (Dth)": summary.weather_variance, "Customers (Dth)": summary.customer_variance, "Other (Dth)": summary.other_variance,
        "Actual vs plan": summary.total_variance_pct, "Re-forecast vs plan": summary.reforecast_vs_plan_pct,
        "Status": summary.status + np.where(summary.signal == "early signal", " (early)", ""),
    })
    st.markdown("**Variance to date, in three parts**")
    st.dataframe(view, hide_index=True, width="stretch", column_config={
        **{c: st.column_config.NumberColumn(format="%,.0f") for c in view.columns[1:6]},
        **{c: st.column_config.NumberColumn(format="%+.1f%%") for c in ["Actual vs plan", "Re-forecast vs plan"]}})
    st.caption("Weather = the model on the actual weather with the planned customers, minus the plan. Customers = the model with "
               "today's customers minus the same with the planned customers. Other = actual minus the model with both "
               "(occupancy, behavior). The three add up to actual minus plan. Status uses the re-forecast P50 against the "
               "published P50: on plan within 3%, watch 3 to 7%, off plan above 7%.")

    region = st.selectbox("Region", REGIONS, format_func=REGION_NAMES.get)
    r = daily[daily.region_id == region].sort_values("gas_day").copy()
    r["gas_day"] = pd.to_datetime(r.gas_day)
    last_fc = r[r.segment == "nws_forecast"].gas_day.max()
    end = last_fc if pd.notna(last_fc) else pd.Timestamp(as_of)
    r = r[r.gas_day <= end]
    x = alt.X("gas_day:T", title=None, axis=alt.Axis(format="%b %d"))
    left, right = st.columns(2)

    series = pd.concat([
        r.assign(series="Plan", dth=r.cum_plan),
        r[r.segment == "actual"].assign(series="Actual", dth=r.cum_actual),
        r[(r.segment != "actual") | (r.gas_day == pd.Timestamp(as_of))].assign(series="Outlook (forecast)", dth=r.cum_outlook),
    ])[["gas_day", "series", "dth", "segment"]]
    colors = alt.Scale(domain=["Plan", "Actual", "Outlook (forecast)"], range=["#9AA0A6", "#1A73E8", "#E8710A"])
    shade = alt.Chart(pd.DataFrame({"start": [pd.Timestamp(as_of)], "end": [end]})).mark_rect(opacity=0.08, color="#E8710A").encode(
        x="start:T", x2="end:T")
    left.markdown(f"**{REGION_NAMES[region]}: cumulative volume (Dth), shaded days are forecast**")
    left.altair_chart((shade + alt.Chart(series).mark_line(point=alt.OverlayMarkDef(size=12)).encode(
        x=x, y=alt.Y("dth:Q", title=None, axis=alt.Axis(format="~s")),
        color=alt.Color("series:N", title=None, scale=colors, legend=alt.Legend(orient="top")),
        strokeDash=alt.condition(alt.datum.series == "Actual", alt.value([1, 0]), alt.value([5, 4])),
    )).properties(height=300), width="stretch")

    a = r[r.segment == "actual"].copy()
    if a.empty:
        right.markdown(f"**{REGION_NAMES[region]}: cumulative variance to plan (Dth)**")
        right.info("The variance split starts with the first day of November actuals.")
    for c, label in (("weather_part", "Weather"), ("customers_part", "Customers"), ("other_part", "Other")):
        a[label] = a[c].cumsum()
    v = a.melt(id_vars="gas_day", value_vars=["Weather", "Customers", "Other"], var_name="part", value_name="dth")
    right.markdown(f"**{REGION_NAMES[region]}: cumulative variance to plan (Dth)**")
    right.altair_chart(alt.Chart(v).mark_area(opacity=0.8).encode(
        x=x, y=alt.Y("dth:Q", title=None, stack="zero", axis=alt.Axis(format="~s")),
        color=alt.Color("part:N", title=None, scale=alt.Scale(domain=["Weather", "Customers", "Other"],
                                                             range=["#E8710A", "#188038", "#9334E6"]),
                        legend=alt.Legend(orient="top")),
    ).properties(height=300), width="stretch")

    h = history[history.region_id == region].sort_values("as_of_date").copy()
    if len(h) > 1:
        h["as_of_date"] = pd.to_datetime(h.as_of_date)
        st.markdown(f"**{REGION_NAMES[region]}: season re-forecast by run (Dth). Real days replace forecast days, "
                    "so the P10 to P90 band narrows; the dashed line is the published P50.**")
        xr = alt.X("as_of_date:T", title="As of", axis=alt.Axis(format="%b %d"))
        band = alt.Chart(h).mark_area(opacity=0.25, color="#1A73E8").encode(
            x=xr, y=alt.Y("reforecast_p10:Q", title=None, axis=alt.Axis(format="~s"), scale=alt.Scale(zero=False)),
            y2="reforecast_p90:Q")
        mid = alt.Chart(h).mark_line(point=True, color="#1A73E8").encode(x=xr, y="reforecast_p50:Q")
        plan_line = alt.Chart(h).mark_rule(strokeDash=[5, 4], color="#5F6368").encode(y="mean(published_p50):Q")
        st.altair_chart((band + mid + plan_line).properties(height=220), width="stretch")

    st.subheader("Ask Genie")
    if GENIE_SPACE_ID == "none":
        st.info("Genie Agent not configured.")
    else:
        st.caption(f"[Open the Genie Agent in Databricks]({cfg.host}/genie/rooms/{GENIE_SPACE_ID})")
        examples = {
            "On track this winter? Biggest risk?":
                "Are we on track for this winter's volume, and where's our biggest risk?",
            "Eastern Plains since Nov 1: weather, customers, other":
                "Show Eastern Plains' daily volume against expected since November 1, split into weather, customer growth, and other.",
            "Enough firm transportation for a design day?":
                "Do we have enough firm transportation for a design day, and where are we short?",
        }
        b = st.columns(len(examples))
        picked = next((q for (label, q), c in zip(examples.items(), b) if c.button(label, width="stretch")), None)
        question = st.chat_input("Ask about the plan, the re-forecast, the design day, or the backtest") or picked
        if question:
            with st.chat_message("user"):
                st.write(question)
            with st.chat_message("assistant"), st.spinner("Genie is working"):
                msg = w.genie.start_conversation_and_wait(GENIE_SPACE_ID, question)
                for att in msg.attachments or []:
                    if att.text and att.text.content:
                        st.markdown(att.text.content)
                    if att.query:
                        if att.query.description:
                            st.markdown(att.query.description)
                        res = w.genie.get_message_attachment_query_result(
                            GENIE_SPACE_ID, msg.conversation_id, msg.message_id, att.attachment_id)
                        sr = res.statement_response
                        cols_ = [c.name for c in sr.manifest.schema.columns]
                        st.dataframe(pd.DataFrame(sr.result.data_array or [], columns=cols_), hide_index=True)
                        with st.expander("Generated SQL"):
                            st.code(att.query.query, language="sql")
