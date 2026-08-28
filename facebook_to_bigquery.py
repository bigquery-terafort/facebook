"""
Facebook → BigQuery  ·  COMPLETE PIPELINE v3.4
===============================================
v2.1 → v3.4 — DATA-LOSS FIXES

──────────────────────────────────────────────────────────────────────────────
JO HUA (2026-08-27, saabit shuda)
──────────────────────────────────────────────────────────────────────────────
  Account 2032090687684703 "TF-Apps Zeeshan Ali New" ka poora data ur gaya:

      30-din window ke ANDAR (07-28 → 08-27):  rows=0     🔴 DELETE hua
      window se PEHLE      (07-16 → 07-27):    rows=1280  ✅ bacha
      campaigns / adsets / ads:                rows=0     🔴 TRUNCATE ne saaf kiya

      Nuqsan: $1,411.25 · 221 ads

  Window ke bahar ka data bach gaya — yehi saabit karta hai ke DELETE ne
  window ke andar ka data uda diya aur wo account load-list mein na hone ki
  wajah se wapas nahi aaya.

──────────────────────────────────────────────────────────────────────────────
6 BUGS — sab v3 mein theek
──────────────────────────────────────────────────────────────────────────────
  BUG 1  DELETE saare accounts ka, LOAD sirf maujooda accounts ka
         v2.1: DELETE FROM t WHERE date BETWEEN start AND end
               (koi account_id filter nahi)
         v3:   DELETE ... AND account_id IN UNNEST(@ok_accounts)
               jahan ok_accounts = sirf wo accounts jo IS RUN mein
               KAAMYABI se fetch huye.

  BUG 2  fail aur "koi data nahi" mein farq nahi
         v2.1: dono soorat mein `return []`
         v3:   fail → `return None` (bilkul alag). None wale account ka
               DELETE hota hi nahi — uska purana data mehfooz rehta hai.

  BUG 3  polling loop mein koi timeout nahi (08-21 ka run 80% pe atka,
         1 ghante baad GitHub ne maara)
         v3:   MAX_POLL_SECONDS (default 900) + progress-stall detection.

  BUG 4  account discovery mein na error check, na pagination
         v2.1: requests.get(...).json().get("data", [])
               API error → khamoshi se [] → sab kuch DELETE
         v3:   "error" key check, raise_for_status, paging.next follow,
               aur khali list pe sys.exit(1).

  BUG 5  status == 1 filter — disabled/unsettled account list se gir jata
         hai, aur BUG 1 uska data uda deta hai
         v3:   saare accounts process hote hain (status sirf log hota hai).
               ACTIVE_ONLY=1 se purana behaviour wapas laya ja sakta hai.

  BUG 6  script kabhi fail nahi hoti — exit code hamesha 0
         v3:   har fail track hota hai; aakhir mein sys.exit(1).
               Workflow ab RED dikhega jab data mein masla ho.

  + TRUNCATE guard: dimension tables (campaigns/adsets/ads/creatives/
    audiences) tabhi truncate hote hain jab HAR account kaamyab ho.
    Warna atomic per-account replace hota hai.

──────────────────────────────────────────────────────────────────────────────
NAYE ENV VARS (sab optional — defaults mehfooz hain)
──────────────────────────────────────────────────────────────────────────────
  MAX_POLL_SECONDS   900   ek async job kitni der tak poll ho (15 min)
  POLL_INTERVAL      10    poll ke beech waqfa
  ACTIVE_ONLY        0     1 = sirf status==1 accounts (v2.1 wala behaviour)
  ALLOW_TRUNCATE     1     0 = dimension tables kabhi truncate na hon
  DRY_RUN            0     1 = fetch to karo lekin BQ ko haath na lagao
──────────────────────────────────────────────────────────────────────────────
"""

import sys
print("Python version:", sys.version, flush=True)
print("Starting imports...", flush=True)

import os, json, logging, time, re
from datetime import datetime, timedelta

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.page import Page
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.customaudience import CustomAudience

from google.cloud import bigquery
from google.oauth2 import service_account

import requests

print("All imports done.", flush=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── ENV VARS ────────────────────────────────────────────────────────────────
FB_APP_ID            = os.environ["FB_APP_ID"]
FB_APP_SECRET        = os.environ["FB_APP_SECRET"]
FB_ACCESS_TOKEN      = os.environ["FB_ACCESS_TOKEN"]
FB_BUSINESS_ID       = os.environ["FB_BUSINESS_ID"]
FB_PAGE_ID           = os.environ.get("FB_PAGE_ID", "")
FB_PIXEL_ID          = os.environ.get("FB_PIXEL_ID", "")
GCP_PROJECT          = os.environ["GCP_PROJECT"]
BQ_DATASET           = os.environ.get("BQ_DATASET", "facebook_data")
GCP_CREDENTIALS_JSON = os.environ["GCP_CREDENTIALS_JSON"]
LOOKBACK_DAYS        = int(os.environ.get("LOOKBACK_DAYS", "7"))

# 🆕 v3
MAX_POLL_SECONDS = int(os.environ.get("MAX_POLL_SECONDS", "900"))
POLL_INTERVAL    = int(os.environ.get("POLL_INTERVAL", "10"))
ACTIVE_ONLY      = os.environ.get("ACTIVE_ONLY", "0") == "1"
ALLOW_TRUNCATE   = os.environ.get("ALLOW_TRUNCATE", "1") == "1"
DRY_RUN          = os.environ.get("DRY_RUN", "0") == "1"

# 🆕 v3 — poore run ka sehat-nama. Koi bhi entry = exit(1).
FAILURES = []

def record_failure(where, detail):
    """Har masla yahan darj hota hai. Aakhir mein exit code tay karta hai."""
    msg = f"{where}: {detail}"
    FAILURES.append(msg)
    log.error(f"  ❌ {msg}")

# ─── ACTION TYPES ────────────────────────────────────────────────────────────
INSTALL_ACTIONS  = {"mobile_app_install", "app_install"}
PURCHASE_ACTIONS = {"offsite_conversion.fb_pixel_purchase", "purchase", "omni_purchase"}
LEAD_ACTIONS     = {"lead", "offsite_conversion.fb_pixel_lead"}
ROAS_ACTIONS     = {"omni_purchase", "offsite_conversion.fb_pixel_purchase", "purchase"}
ADD_CART_ACTIONS = {"add_to_cart", "offsite_conversion.fb_pixel_add_to_cart"}
CHECKOUT_ACTIONS = {"initiate_checkout", "offsite_conversion.fb_pixel_initiate_checkout"}
TRIAL_ACTIONS    = {"start_trial", "subscribe"}

# ─── INSIGHT FIELDS ──────────────────────────────────────────────────────────
INSIGHT_FIELDS = [
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.adset_id,
    AdsInsights.Field.adset_name,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.ad_name,
    AdsInsights.Field.account_id,
    AdsInsights.Field.account_name,
    AdsInsights.Field.objective,
    AdsInsights.Field.buying_type,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.spend,
    AdsInsights.Field.reach,
    AdsInsights.Field.frequency,
    AdsInsights.Field.cpc,
    AdsInsights.Field.cpm,
    AdsInsights.Field.ctr,
    AdsInsights.Field.cpp,
    AdsInsights.Field.unique_clicks,
    AdsInsights.Field.unique_ctr,
    AdsInsights.Field.actions,
    AdsInsights.Field.action_values,
    AdsInsights.Field.cost_per_action_type,
    AdsInsights.Field.video_p25_watched_actions,
    AdsInsights.Field.video_p50_watched_actions,
    AdsInsights.Field.video_p75_watched_actions,
    AdsInsights.Field.video_p100_watched_actions,
    AdsInsights.Field.outbound_clicks,
    AdsInsights.Field.outbound_clicks_ctr,
    AdsInsights.Field.quality_ranking,
    AdsInsights.Field.engagement_rate_ranking,
    AdsInsights.Field.conversion_rate_ranking,
    AdsInsights.Field.inline_post_engagement,
    AdsInsights.Field.inline_link_clicks,
    AdsInsights.Field.inline_link_click_ctr,
    AdsInsights.Field.attribution_setting,
]

# ─── BQ SCHEMAS ──────────────────────────────────────────────────────────────
def kpi_fields():
    return [
        bigquery.SchemaField("impressions",           "INTEGER"),
        bigquery.SchemaField("clicks",                "INTEGER"),
        bigquery.SchemaField("spend",                 "FLOAT"),
        bigquery.SchemaField("reach",                 "INTEGER"),
        bigquery.SchemaField("frequency",             "FLOAT"),
        bigquery.SchemaField("cpc",                   "FLOAT"),
        bigquery.SchemaField("cpm",                   "FLOAT"),
        bigquery.SchemaField("ctr",                   "FLOAT"),
        bigquery.SchemaField("cpp",                   "FLOAT"),
        bigquery.SchemaField("unique_clicks",         "INTEGER"),
        bigquery.SchemaField("unique_ctr",            "FLOAT"),
        bigquery.SchemaField("mobile_app_installs",   "INTEGER"),
        bigquery.SchemaField("cost_per_install",      "FLOAT"),
        bigquery.SchemaField("purchases",             "INTEGER"),
        bigquery.SchemaField("purchase_value",        "FLOAT"),
        bigquery.SchemaField("cost_per_purchase",     "FLOAT"),
        bigquery.SchemaField("roas",                  "FLOAT"),
        bigquery.SchemaField("leads",                 "INTEGER"),
        bigquery.SchemaField("add_to_cart",           "INTEGER"),
        bigquery.SchemaField("initiate_checkout",     "INTEGER"),
        bigquery.SchemaField("trials_started",        "INTEGER"),
        bigquery.SchemaField("outbound_clicks",       "INTEGER"),
        bigquery.SchemaField("outbound_ctr",          "FLOAT"),
        bigquery.SchemaField("video_p25_views",       "INTEGER"),
        bigquery.SchemaField("video_p50_views",       "INTEGER"),
        bigquery.SchemaField("video_p75_views",       "INTEGER"),
        bigquery.SchemaField("video_p100_views",      "INTEGER"),
        bigquery.SchemaField("quality_ranking",            "STRING"),
        bigquery.SchemaField("engagement_rate_ranking",    "STRING"),
        bigquery.SchemaField("conversion_rate_ranking",    "STRING"),
        bigquery.SchemaField("inline_post_engagement",     "INTEGER"),
        bigquery.SchemaField("inline_link_clicks",         "INTEGER"),
        bigquery.SchemaField("inline_link_click_ctr",      "FLOAT"),
        bigquery.SchemaField("attribution_setting",        "STRING"),
    ]

SCHEMAS = {
    "account_daily": [
        bigquery.SchemaField("date_start",      "DATE"),
        bigquery.SchemaField("account_id",      "STRING"),
        bigquery.SchemaField("account_name",    "STRING"),
        *kpi_fields(),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "campaign_daily_insights": [
        bigquery.SchemaField("date_start",      "DATE"),
        bigquery.SchemaField("account_id",      "STRING"),
        bigquery.SchemaField("campaign_id",     "STRING"),
        bigquery.SchemaField("campaign_name",   "STRING"),
        bigquery.SchemaField("objective",       "STRING"),
        bigquery.SchemaField("buying_type",     "STRING"),
        *kpi_fields(),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "adset_daily_insights": [
        bigquery.SchemaField("date_start",      "DATE"),
        bigquery.SchemaField("account_id",      "STRING"),
        bigquery.SchemaField("campaign_id",     "STRING"),
        bigquery.SchemaField("campaign_name",   "STRING"),
        bigquery.SchemaField("adset_id",        "STRING"),
        bigquery.SchemaField("adset_name",      "STRING"),
        bigquery.SchemaField("objective",       "STRING"),
        bigquery.SchemaField("buying_type",     "STRING"),
        *kpi_fields(),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "ad_insights_daily": [
        bigquery.SchemaField("date_start",      "DATE"),
        bigquery.SchemaField("date_stop",       "DATE"),
        bigquery.SchemaField("account_id",      "STRING"),
        bigquery.SchemaField("account_name",    "STRING"),
        bigquery.SchemaField("campaign_id",     "STRING"),
        bigquery.SchemaField("campaign_name",   "STRING"),
        bigquery.SchemaField("adset_id",        "STRING"),
        bigquery.SchemaField("adset_name",      "STRING"),
        bigquery.SchemaField("ad_id",           "STRING"),
        bigquery.SchemaField("ad_name",         "STRING"),
        bigquery.SchemaField("objective",       "STRING"),
        bigquery.SchemaField("buying_type",     "STRING"),
        *kpi_fields(),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "ad_insights_by_country": [
        bigquery.SchemaField("date_start",      "DATE"),
        bigquery.SchemaField("account_id",      "STRING"),
        bigquery.SchemaField("campaign_id",     "STRING"),
        bigquery.SchemaField("adset_id",        "STRING"),
        bigquery.SchemaField("ad_id",           "STRING"),
        bigquery.SchemaField("country",         "STRING"),
        *kpi_fields(),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "ad_insights_by_device": [
        bigquery.SchemaField("date_start",          "DATE"),
        bigquery.SchemaField("account_id",          "STRING"),
        bigquery.SchemaField("campaign_id",         "STRING"),
        bigquery.SchemaField("adset_id",            "STRING"),
        bigquery.SchemaField("ad_id",               "STRING"),
        bigquery.SchemaField("device_platform",     "STRING"),
        bigquery.SchemaField("impression_device",   "STRING"),
        *kpi_fields(),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "ad_insights_by_placement": [
        bigquery.SchemaField("date_start",          "DATE"),
        bigquery.SchemaField("account_id",          "STRING"),
        bigquery.SchemaField("campaign_id",         "STRING"),
        bigquery.SchemaField("adset_id",            "STRING"),
        bigquery.SchemaField("ad_id",               "STRING"),
        bigquery.SchemaField("publisher_platform",  "STRING"),
        bigquery.SchemaField("platform_position",   "STRING"),
        bigquery.SchemaField("impression_device",   "STRING"),
        *kpi_fields(),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "ad_insights_by_age_gender": [
        bigquery.SchemaField("date_start",      "DATE"),
        bigquery.SchemaField("account_id",      "STRING"),
        bigquery.SchemaField("campaign_id",     "STRING"),
        bigquery.SchemaField("adset_id",        "STRING"),
        bigquery.SchemaField("ad_id",           "STRING"),
        bigquery.SchemaField("age",             "STRING"),
        bigquery.SchemaField("gender",          "STRING"),
        *kpi_fields(),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "campaigns": [
        bigquery.SchemaField("account_id",          "STRING"),
        bigquery.SchemaField("campaign_id",         "STRING"),
        bigquery.SchemaField("name",                "STRING"),
        bigquery.SchemaField("status",              "STRING"),
        bigquery.SchemaField("effective_status",    "STRING"),
        bigquery.SchemaField("objective",           "STRING"),
        bigquery.SchemaField("buying_type",         "STRING"),
        bigquery.SchemaField("bid_strategy",        "STRING"),
        bigquery.SchemaField("daily_budget",        "FLOAT"),
        bigquery.SchemaField("lifetime_budget",     "FLOAT"),
        bigquery.SchemaField("budget_remaining",    "FLOAT"),
        bigquery.SchemaField("spend_cap",           "FLOAT"),
        bigquery.SchemaField("start_time",          "TIMESTAMP"),
        bigquery.SchemaField("stop_time",           "TIMESTAMP"),
        bigquery.SchemaField("created_time",        "TIMESTAMP"),
        bigquery.SchemaField("updated_time",        "TIMESTAMP"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "adsets": [
        bigquery.SchemaField("account_id",                      "STRING"),
        bigquery.SchemaField("adset_id",                        "STRING"),
        bigquery.SchemaField("campaign_id",                     "STRING"),
        bigquery.SchemaField("name",                            "STRING"),
        bigquery.SchemaField("status",                          "STRING"),
        bigquery.SchemaField("effective_status",                "STRING"),
        bigquery.SchemaField("optimization_goal",               "STRING"),
        bigquery.SchemaField("billing_event",                   "STRING"),
        bigquery.SchemaField("bid_strategy",                    "STRING"),
        bigquery.SchemaField("bid_amount",                      "FLOAT"),
        bigquery.SchemaField("daily_budget",                    "FLOAT"),
        bigquery.SchemaField("lifetime_budget",                 "FLOAT"),
        bigquery.SchemaField("targeting_countries",             "STRING"),
        bigquery.SchemaField("targeting_age_min",               "INTEGER"),
        bigquery.SchemaField("targeting_age_max",               "INTEGER"),
        bigquery.SchemaField("targeting_genders",               "STRING"),
        bigquery.SchemaField("targeting_custom_audiences",      "STRING"),
        bigquery.SchemaField("placements_publisher_platforms",  "STRING"),
        bigquery.SchemaField("promoted_object_app_id",          "STRING"),
        bigquery.SchemaField("promoted_object_pixel_id",        "STRING"),
        bigquery.SchemaField("promoted_object_object_store_url", "STRING"),
        bigquery.SchemaField("promoted_object_android_package",  "STRING"),
        bigquery.SchemaField("promoted_object_apple_app_store_id","STRING"),
        bigquery.SchemaField("start_time",                      "TIMESTAMP"),
        bigquery.SchemaField("end_time",                        "TIMESTAMP"),
        bigquery.SchemaField("created_time",                    "TIMESTAMP"),
        bigquery.SchemaField("updated_time",                    "TIMESTAMP"),
        bigquery.SchemaField("_ingested_at",                    "TIMESTAMP"),
    ],
    "ads": [
        bigquery.SchemaField("account_id",              "STRING"),
        bigquery.SchemaField("ad_id",                   "STRING"),
        bigquery.SchemaField("adset_id",                "STRING"),
        bigquery.SchemaField("campaign_id",             "STRING"),
        bigquery.SchemaField("name",                    "STRING"),
        bigquery.SchemaField("status",                  "STRING"),
        bigquery.SchemaField("effective_status",        "STRING"),
        bigquery.SchemaField("creative_id",             "STRING"),
        bigquery.SchemaField("creative_title",          "STRING"),
        bigquery.SchemaField("creative_body",           "STRING"),
        bigquery.SchemaField("creative_call_to_action", "STRING"),
        bigquery.SchemaField("created_time",            "TIMESTAMP"),
        bigquery.SchemaField("updated_time",            "TIMESTAMP"),
        bigquery.SchemaField("_ingested_at",            "TIMESTAMP"),
    ],
    "ad_creatives": [
        bigquery.SchemaField("account_id",               "STRING"),
        bigquery.SchemaField("creative_id",              "STRING"),
        bigquery.SchemaField("name",                     "STRING"),
        bigquery.SchemaField("title",                    "STRING"),
        bigquery.SchemaField("body",                     "STRING"),
        bigquery.SchemaField("call_to_action_type",      "STRING"),
        bigquery.SchemaField("image_url",                "STRING"),
        bigquery.SchemaField("thumbnail_url",            "STRING"),
        bigquery.SchemaField("video_id",                 "STRING"),
        bigquery.SchemaField("link_url",                 "STRING"),
        bigquery.SchemaField("effective_object_story_id","STRING"),
        bigquery.SchemaField("_ingested_at",             "TIMESTAMP"),
    ],
    "ad_delivery": [
        bigquery.SchemaField("date_start",                  "DATE"),
        bigquery.SchemaField("account_id",                  "STRING"),
        bigquery.SchemaField("campaign_id",                 "STRING"),
        bigquery.SchemaField("campaign_name",               "STRING"),
        bigquery.SchemaField("adset_id",                    "STRING"),
        bigquery.SchemaField("adset_name",                  "STRING"),
        bigquery.SchemaField("ad_id",                       "STRING"),
        bigquery.SchemaField("ad_name",                     "STRING"),
        bigquery.SchemaField("quality_ranking",             "STRING"),
        bigquery.SchemaField("engagement_rate_ranking",     "STRING"),
        bigquery.SchemaField("conversion_rate_ranking",     "STRING"),
        bigquery.SchemaField("impressions",                 "INTEGER"),
        bigquery.SchemaField("spend",                       "FLOAT"),
        bigquery.SchemaField("_ingested_at",                "TIMESTAMP"),
    ],
    "auction_insights": [
        bigquery.SchemaField("date_start",              "DATE"),
        bigquery.SchemaField("account_id",              "STRING"),
        bigquery.SchemaField("campaign_id",             "STRING"),
        bigquery.SchemaField("campaign_name",           "STRING"),
        bigquery.SchemaField("adset_id",                "STRING"),
        bigquery.SchemaField("adset_name",              "STRING"),
        bigquery.SchemaField("impression_share",        "FLOAT"),
        bigquery.SchemaField("outranking_share",        "FLOAT"),
        bigquery.SchemaField("overlap_rate",            "FLOAT"),
        bigquery.SchemaField("position_above_rate",     "FLOAT"),
        bigquery.SchemaField("_ingested_at",            "TIMESTAMP"),
    ],
    "reach_frequency": [
        bigquery.SchemaField("date_start",          "DATE"),
        bigquery.SchemaField("account_id",          "STRING"),
        bigquery.SchemaField("campaign_id",         "STRING"),
        bigquery.SchemaField("campaign_name",       "STRING"),
        bigquery.SchemaField("adset_id",            "STRING"),
        bigquery.SchemaField("adset_name",          "STRING"),
        bigquery.SchemaField("reach",               "INTEGER"),
        bigquery.SchemaField("frequency",           "FLOAT"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("spend",               "FLOAT"),
        bigquery.SchemaField("cpp",                 "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "app_events": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("account_id",      "STRING"),
        bigquery.SchemaField("app_id",          "STRING"),
        bigquery.SchemaField("event_name",      "STRING"),
        bigquery.SchemaField("count",           "INTEGER"),
        bigquery.SchemaField("unique_users",    "INTEGER"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "pixel_events": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("account_id",      "STRING"),
        bigquery.SchemaField("event_name",      "STRING"),
        bigquery.SchemaField("count",           "INTEGER"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "page_insights": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("page_id",         "STRING"),
        bigquery.SchemaField("metric_name",     "STRING"),
        bigquery.SchemaField("value",           "FLOAT"),
        bigquery.SchemaField("period",          "STRING"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "custom_audiences": [
        bigquery.SchemaField("account_id",          "STRING"),
        bigquery.SchemaField("audience_id",         "STRING"),
        bigquery.SchemaField("name",                "STRING"),
        bigquery.SchemaField("subtype",             "STRING"),
        bigquery.SchemaField("approximate_count",   "INTEGER"),
        bigquery.SchemaField("data_source",         "STRING"),
        bigquery.SchemaField("lookalike_spec",      "STRING"),
        bigquery.SchemaField("retention_days",      "INTEGER"),
        bigquery.SchemaField("created_time",        "TIMESTAMP"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
}

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def safe_float(v):
    try: return float(v) if v not in (None, "") else None
    except: return None

def safe_int(v):
    try: return int(float(v)) if v not in (None, "") else None
    except: return None

def now_ts():
    return datetime.utcnow().isoformat()

def parse_ts(ts):
    if not ts: return None
    ts = str(ts).replace("T", " ")
    ts = re.sub(r'[+-]\d{4}$', '', ts).strip()
    ts = re.sub(r'[+-]\d{2}:\d{2}$', '', ts).strip()
    return ts

def norm_acct(acct_id):
    """`act_123` aur `123` ko ek hi shakl mein laata hai.
    Insights `account_id` bina prefix ke deta hai, AdAccount.get_id() prefix ke saath.
    DELETE guard ke liye dono ka match zaroori hai."""
    if acct_id is None:
        return None
    s = str(acct_id).strip()
    return s[4:] if s.startswith("act_") else s

def extract_package_from_store_url(url):
    """promoted_object.object_store_url se android package ya apple id nikalta hai."""
    if not url:
        return None, None
    android_match = re.search(r'[?&]id=([\w.]+)', url)
    if android_match:
        return android_match.group(1), None
    apple_match = re.search(r'/id(\d+)', url)
    if apple_match:
        return None, apple_match.group(1)
    return None, None

def date_range():
    end   = datetime.utcnow().date()
    start = end - timedelta(days=LOOKBACK_DAYS)
    return str(start), str(end)

def extract_actions(insight, action_types):
    return sum(
        (safe_int(a.get("value")) or 0)
        for a in insight.get("actions", [])
        if a.get("action_type") in action_types
    )

def extract_action_values(insight, action_types):
    return sum(
        (safe_float(av.get("value")) or 0.0)
        for av in insight.get("action_values", [])
        if av.get("action_type") in action_types
    )

def extract_cost_per_action(insight, action_types):
    for cpa in insight.get("cost_per_action_type", []):
        if cpa.get("action_type") in action_types:
            return safe_float(cpa.get("value"))
    return None

def extract_video(insight, field):
    for v in insight.get(field, []):
        if v.get("action_type") == "video_view":
            return safe_int(v.get("value"))
    return None

def build_kpi(insight):
    installs  = extract_actions(insight, INSTALL_ACTIONS)
    purchases = extract_actions(insight, PURCHASE_ACTIONS)
    purch_val = extract_action_values(insight, ROAS_ACTIONS)
    spend     = safe_float(insight.get("spend")) or 0.0

    outbound_clicks = next(
        (safe_int(x.get("value")) for x in insight.get("outbound_clicks", [])
         if x.get("action_type") == "outbound_click"), None
    )
    outbound_ctr = next(
        (safe_float(x.get("value")) for x in insight.get("outbound_clicks_ctr", [])
         if x.get("action_type") == "outbound_click"), None
    )

    return {
        "impressions":              safe_int(insight.get("impressions")),
        "clicks":                   safe_int(insight.get("clicks")),
        "spend":                    spend,
        "reach":                    safe_int(insight.get("reach")),
        "frequency":                safe_float(insight.get("frequency")),
        "cpc":                      safe_float(insight.get("cpc")),
        "cpm":                      safe_float(insight.get("cpm")),
        "ctr":                      safe_float(insight.get("ctr")),
        "cpp":                      safe_float(insight.get("cpp")),
        "unique_clicks":            safe_int(insight.get("unique_clicks")),
        "unique_ctr":               safe_float(insight.get("unique_ctr")),
        "mobile_app_installs":      installs,
        "cost_per_install":         round(spend / installs, 4) if installs else None,
        "purchases":                purchases,
        "purchase_value":           purch_val,
        "cost_per_purchase":        extract_cost_per_action(insight, PURCHASE_ACTIONS),
        "roas":                     round(purch_val / spend, 4) if spend and purch_val else None,
        "leads":                    extract_actions(insight, LEAD_ACTIONS),
        "add_to_cart":              extract_actions(insight, ADD_CART_ACTIONS),
        "initiate_checkout":        extract_actions(insight, CHECKOUT_ACTIONS),
        "trials_started":           extract_actions(insight, TRIAL_ACTIONS),
        "outbound_clicks":          outbound_clicks,
        "outbound_ctr":             outbound_ctr,
        "video_p25_views":          extract_video(insight, "video_p25_watched_actions"),
        "video_p50_views":          extract_video(insight, "video_p50_watched_actions"),
        "video_p75_views":          extract_video(insight, "video_p75_watched_actions"),
        "video_p100_views":         extract_video(insight, "video_p100_watched_actions"),
        "quality_ranking":          insight.get("quality_ranking"),
        "engagement_rate_ranking":  insight.get("engagement_rate_ranking"),
        "conversion_rate_ranking":  insight.get("conversion_rate_ranking"),
        "inline_post_engagement":   safe_int(insight.get("inline_post_engagement")),
        "inline_link_clicks":       safe_int(insight.get("inline_link_clicks")),
        "inline_link_click_ctr":    safe_float(insight.get("inline_link_click_ctr")),
        "attribution_setting":      insight.get("attribution_setting"),
    }

# ─── BQ HELPERS ──────────────────────────────────────────────────────────────
def get_bq_client():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_CREDENTIALS_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(project=GCP_PROJECT, credentials=creds)

def ensure_dataset(client):
    try:
        client.get_dataset(BQ_DATASET)
    except Exception:
        log.info(f"Creating dataset {BQ_DATASET}")
        client.create_dataset(bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}"))

def ensure_table(client, name):
    ref = client.dataset(BQ_DATASET).table(name)
    try:
        client.get_table(ref)
    except Exception:
        log.info(f"Creating table {name}")
        client.create_table(bigquery.Table(ref, schema=SCHEMAS[name]))

DATE_COL_MAP = {
    "ad_insights_daily":        "date_start",
    "ad_insights_by_country":   "date_start",
    "ad_insights_by_device":    "date_start",
    "ad_insights_by_placement": "date_start",
    "ad_insights_by_age_gender":"date_start",
    "account_daily":            "date_start",
    "campaign_daily_insights":  "date_start",
    "adset_daily_insights":     "date_start",
    "ad_delivery":              "date_start",
    "auction_insights":         "date_start",
    "reach_frequency":          "date_start",
    "pixel_events":             "date",
    "app_events":               "date",
    "page_insights":            "date",
}

# 🛡️ Ye tables account-scoped NAHI hain — inpe account guard nahi lag sakta.
NO_ACCOUNT_TABLES = {"page_insights"}


def _load_job(client, table_ref, rows, name, write_disposition):
    """
    🆕 v3.1 — streaming insert ki jagah LOAD JOB.

    Load jobs:
      · atomic (poora chalta hai ya bilkul nahi)
      · streaming buffer use NAHI karte
      · muft hain (streaming inserts paise lete hain)
      · job.output_rows se tasdeeq ho jati hai

    🛡️ BUG 7 KA FIX — TRUNCATE + streaming insert = KHAMOSH ROW LOSS
       Saabit (2026-08-27 ka run):
           adsets  log: 712 load   → table: 567   (−20%)
           ads     log: 2,417 load → table: 2,010 (−17%)
           account_daily (DELETE wala) 465 → 465   ✅ poora

       BigQuery ka TRUNCATE streaming buffer ka metadata reset kar deta hai;
       foran baad ke insert_rows_json rows bina koi error diye gir jate hain.
       Ye bug v2.1 mein BHI tha — sirf kisi ne pakda nahi.
    """
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMAS[name],
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()                      # error pe raise karta hai
    return job.output_rows


def load_to_bq(client, name, result):
    """
    🆕 v3 — DELETE ab account-scoped hai.

    `result` ek tuple hai: (rows, ok_accounts)
        rows        — jo BQ mein daalne hain
        ok_accounts — un accounts ka set jo IS RUN mein KAAMYABI se fetch huye
                      (normalized, bina `act_` prefix ke)

    🛡️ BUG 1 KA FIX:
       v2.1 poore date-range ka DELETE karta tha, phir sirf maujooda accounts
       load karta tha. Jo account list se girta (disabled hua, access gayi,
       ya fetch fail hua), uska data UR JATA tha aur wapas nahi aata.
       Saabit: account 2032090687684703 ka 30-din window ka data rows=0 ho
       gaya, jabke window se bahar ka 1,280 rows bach gaya.

       Ab DELETE mein `AND account_id IN UNNEST(@ok)` lagta hai. Jo account
       is run mein fetch NAHI hua, uska purana data bilkul nahi chhua jata.

    🛡️ BUG 2 KA FIX (yahan ka hissa):
       Agar ok_accounts KHALI hai to DELETE bilkul nahi hota — chahe rows
       kitne bhi hon.

    🛡️ BUG 7 KA FIX (v3.1):
       Load jobs — TRUNCATE+streaming wala 20% row loss khatam.
    """
    rows, ok_accounts = result if isinstance(result, tuple) else (result, None)

    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{name}"
    start, end = date_range()
    date_col = DATE_COL_MAP.get(name)

    if DRY_RUN:
        log.info(f"  [DRY_RUN] {name}: {len(rows or []):,} rows, "
                 f"{len(ok_accounts) if ok_accounts is not None else 'n/a'} ok accounts — BQ chhua nahi")
        return

    if not rows:
        # 🛡️ Khali nateeja pe kabhi DELETE/TRUNCATE nahi — purana data mehfooz.
        log.warning(f"  ⚠️  {name}: 0 rows — DELETE/TRUNCATE nahi kiya (purana data mehfooz)")
        return

    # ── DATE-BASED TABLES ───────────────────────────────────────────────────
    if date_col:
        if name in NO_ACCOUNT_TABLES:
            # page_insights ka koi account_id nahi
            try:
                client.query(
                    f"DELETE FROM `{table_ref}` WHERE {date_col} BETWEEN '{start}' AND '{end}'"
                ).result()
                log.info(f"  Cleared {name} ({start} → {end})")
            except Exception as e:
                record_failure(f"delete[{name}]", e)
                return
        else:
            if not ok_accounts:
                record_failure(name, "ok_accounts khali — DELETE skip, kuch load nahi kiya")
                return

            ok_list = sorted(ok_accounts)
            try:
                client.query(
                    f"""
                    DELETE FROM `{table_ref}`
                    WHERE {date_col} BETWEEN @start AND @end
                      AND REPLACE(IFNULL(account_id,''), 'act_', '') IN UNNEST(@ok)
                    """,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("start", "DATE", start),
                            bigquery.ScalarQueryParameter("end",   "DATE", end),
                            bigquery.ArrayQueryParameter("ok", "STRING", ok_list),
                        ]
                    ),
                ).result()
                log.info(f"  Cleared {name} ({start} → {end}) "
                         f"for {len(ok_list)} fetched accounts only")
            except Exception as e:
                record_failure(f"delete[{name}]", e)
                return

        write_mode = bigquery.WriteDisposition.WRITE_APPEND

    # ── DIMENSION TABLES ────────────────────────────────────────────────────
    else:
        all_ok = ok_accounts is not None and len(ok_accounts) == len(ALL_DISCOVERED_ACCOUNTS)

        if ALLOW_TRUNCATE and all_ok:
            # 🆕 v3.1: alag TRUNCATE nahi — load job khud WRITE_TRUNCATE karta hai.
            #          Ye ATOMIC hai, aur streaming-buffer wala 20% loss nahi hota.
            write_mode = bigquery.WriteDisposition.WRITE_TRUNCATE
            log.info(f"  {name}: atomic replace (saare {len(ok_accounts)} accounts kaamyab)")
        else:
            # 🛡️ Kuch account fail — sirf unhi ka data replace karo jo kaamyab hue.
            if not ok_accounts:
                record_failure(name, "ok_accounts khali — replace skip")
                return
            ok_list = sorted(ok_accounts)
            try:
                client.query(
                    f"""
                    DELETE FROM `{table_ref}`
                    WHERE REPLACE(IFNULL(account_id,''), 'act_', '') IN UNNEST(@ok)
                    """,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ArrayQueryParameter("ok", "STRING", ok_list)]
                    ),
                ).result()
                log.warning(f"  ⚠️  {name}: poora replace nahi kiya "
                            f"({len(ok_list)}/{len(ALL_DISCOVERED_ACCOUNTS)} accounts kaamyab) — "
                            f"fail hue accounts ka purana data MEHFOOZ hai")
            except Exception as e:
                record_failure(f"delete[{name}]", e)
                return
            write_mode = bigquery.WriteDisposition.WRITE_APPEND

    # ── LOAD (job — streaming nahi) ─────────────────────────────────────────
    try:
        loaded = _load_job(client, table_ref, rows, name, write_mode)
    except Exception as e:
        record_failure(f"load[{name}]", e)
        return

    # 🛡️ Tasdeeq: jitne bheje, utne hi gaye?
    if loaded != len(rows):
        record_failure(name, f"row count mismatch — bheje {len(rows):,}, gaye {loaded:,}")
    else:
        log.info(f"  ✅ {loaded:,} rows → {name} (verified)")


# ─── ACCOUNT DISCOVERY ───────────────────────────────────────────────────────
ALL_DISCOVERED_ACCOUNTS = set()   # normalized ids — TRUNCATE guard iske saath compare karta hai


# Facebook ke aarzi (transient) error codes — inpe retry karna chahiye
TRANSIENT_CODES    = {1, 2, 4, 17, 32, 341, 613, 80000, 80004}
TRANSIENT_SUBCODES = {99, 2446079}


def _fetch_account_page(url, params, label="account_page"):
    """
    🛡️ BUG 4 KA FIX: v2.1 seedha `.json().get("data", [])` karta tha.
       API error (rate limit, token expiry) pe chup-chaap [] milta tha,
       aur phir sab kuch DELETE ho jata tha.

    🛡️ BUG 10 KA FIX (v3.4) — DISCOVERY PE RETRY + ASLI ERROR
       Saabit (run #186 aur #187, 2026-08-28):
           ❌ account_discovery[owned]: 400 Client Error ... owned_ad_accounts
       Aur ye AARZI tha — 05:07 pe wahi call kaamyab thi, client_ad_accounts
       dono baar theek chala. Yani rate limit.

       v3.3 mein `raise_for_status()` error BODY padhne se PEHLE raise kar
       deta tha — isliye Facebook ka asli code/subcode dikhta hi nahi tha,
       aur retry ka koi mauqa nahi milta tha.

       Ab:
         · body hamesha parse hoti hai (status chahe kuch bhi ho)
         · asli code/subcode/message log hota hai
         · aarzi errors pe 5 retry + backoff (30s, 60s, 90s, 120s, 150s)
         · sirf asli (permanent) error pe raise
    """
    last_err = None
    for attempt in range(5):
        try:
            r = requests.get(url, params=params, timeout=60)

            # 🆕 body pehle padho — status chahe 400 ho ya 200
            try:
                body = r.json()
            except Exception:
                body = {}

            err = body.get("error")
            if err:
                code    = err.get("code")
                sub     = err.get("error_subcode")
                message = err.get("message", "")
                is_transient = (
                    code in TRANSIENT_CODES
                    or sub in TRANSIENT_SUBCODES
                    or any(w in message.lower() for w in
                           ("rate", "too many", "reduce the amount", "temporarily", "try again"))
                )
                if is_transient and attempt < 4:
                    wait = 30 * (attempt + 1)
                    log.warning(f"  {label}: aarzi error code={code}/{sub} — "
                                f"{wait}s wait, retry {attempt+1}/5")
                    log.warning(f"    FB message: {message[:200]}")
                    time.sleep(wait)
                    last_err = RuntimeError(f"code={code} sub={sub}: {message[:200]}")
                    continue
                # permanent — ya retries khatam
                raise RuntimeError(
                    f"Facebook API error (status {r.status_code}) "
                    f"code={code} subcode={sub}: {message[:300]}")

            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code} bina error body: {r.text[:300]}")

            return body

        except RuntimeError:
            raise
        except Exception as e:
            # network / timeout — ye bhi aarzi hai
            if attempt < 4:
                wait = 30 * (attempt + 1)
                log.warning(f"  {label}: network error — {wait}s wait, retry {attempt+1}/5: {e}")
                time.sleep(wait)
                last_err = e
                continue
            raise

    raise RuntimeError(f"{label}: 5 retries ke baad haar gaye — {last_err}")


def _collect_accounts(edge):
    """owned_ad_accounts / client_ad_accounts — poori pagination ke saath."""
    out = []
    url = f"https://graph.facebook.com/v18.0/{FB_BUSINESS_ID}/{edge}"
    params = {"fields": "id,name,account_status", "limit": 100, "access_token": FB_ACCESS_TOKEN}
    page = 0
    while url and page < 50:
        page += 1
        body = _fetch_account_page(
            url,
            params if page == 1 else {"access_token": FB_ACCESS_TOKEN},
            label=f"{edge} page {page}")
        data = body.get("data", [])
        out.extend(data)
        log.info(f"  {edge} page {page}: {len(data)} accounts (total {len(out)})")
        url = body.get("paging", {}).get("next")   # 🆕 v3: pagination follow
    return out


def get_all_ad_accounts():
    """
    🛡️ BUG 4 + BUG 5 KA FIX.
       v2.1 sirf status==1 wale accounts leta tha. Jab koi account disabled
       (2) / unsettled (3) hota, wo list se girta — aur BUG 1 ka global DELETE
       uska data uda deta. Ab saare accounts process hote hain; status sirf
       log hota hai. ACTIVE_ONLY=1 se purana behaviour wapas aa sakta hai.
    """
    global ALL_DISCOVERED_ACCOUNTS
    log.info(f"Discovering ad accounts from Business Manager {FB_BUSINESS_ID}...")

    # 🛡️ BUG 9 KA FIX (v3.2) — ADHOORI DISCOVERY PE FORAN RUKO
    #
    #    Saabit (run #186, 2026-08-28 06:05):
    #        ❌ account_discovery[owned]: 400 Client Error
    #        → sirf 10 accounts mile (15 ki jagah)
    #
    #    v3.1 mein script aage barh jati thi. Khatra ye tha:
    #        ALL_DISCOVERED_ACCOUNTS = 10
    #        agar 10/10 kaamyab → all_ok = True
    #        → campaigns/adsets/ads/ad_creatives/custom_audiences pe
    #          WRITE_TRUNCATE → gayab 5 accounts ki dimension rows UR JATIN
    #        (sirf adsets mein ~145 rows: 6+7+6+39+87)
    #
    #    Ab: koi bhi edge fail ho to BigQuery ko HAATH HI NA LAGE.
    #    Insight tables to account-scoped DELETE se pehle hi mehfooz the,
    #    lekin dimension tables ke liye POORI account list lazmi hai.
    all_accounts = []
    discovery_failed = False

    try:
        all_accounts.extend(_collect_accounts("owned_ad_accounts"))
    except Exception as e:
        record_failure("account_discovery[owned]", e)
        discovery_failed = True

    try:
        for a in _collect_accounts("client_ad_accounts"):
            if not any(x.get("id") == a.get("id") for x in all_accounts):
                all_accounts.append(a)
    except Exception as e:
        record_failure("account_discovery[client]", e)
        discovery_failed = True

    if discovery_failed:
        log.error("=" * 70)
        log.error("🔴 ACCOUNT DISCOVERY ADHOORI RAHI — BigQuery ko HAATH NAHI LAGAYA")
        log.error(f"   Sirf {len(all_accounts)} accounts mile. Poori list ke bagair")
        log.error("   dimension tables (campaigns/adsets/ads/creatives/audiences) ka")
        log.error("   atomic replace gayab accounts ka data uda deta.")
        log.error("   Ye aksar aarzi rate-limit hota hai — thodi der baad dobara chalao.")
        log.error("=" * 70)
        sys.exit(1)

    if not all_accounts:
        # 🛡️ BUG 6: khali list pe kabhi aage mat barho — warna sab kuch ur jayega.
        log.error("🔴 KOI ad account nahi mila — foran ruk rahe hain (data bachane ke liye)")
        sys.exit(1)

    STATUS_NAMES = {1: "active", 2: "disabled", 3: "unsettled", 7: "pending",
                    8: "pending_risk", 9: "grace", 100: "closed", 101: "any"}

    selected = []
    for a in all_accounts:
        status = a.get("account_status")
        sname  = STATUS_NAMES.get(status, "unknown")
        aid    = a.get("id")
        log.info(f"  Account: {a.get('name')} | ID: {aid} | Status: {status} ({sname})")

        if ACTIVE_ONLY and status != 1:
            log.warning(f"    ⏭️  ACTIVE_ONLY=1 — skip (status {sname}). "
                        f"Is account ka purana data CHHUA NAHI jayega.")
            continue

        if status != 1:
            log.warning(f"    ⚠️  Status '{sname}' — phir bhi process kar rahe hain "
                        f"(v2.1 ise skip karta tha aur uska data uda deta tha)")

        ALL_DISCOVERED_ACCOUNTS.add(norm_acct(aid))
        selected.append(AdAccount(aid))

    log.info(f"  Total {len(selected)} accounts process honge "
             f"(discovered: {len(all_accounts)})")
    return selected

# ─── ASYNC INSIGHTS FETCHER ───────────────────────────────────────────────────
def get_insights_async(account, level, breakdowns=None, extra_fields=None, params_extra=None):
    """
    🛡️ BUG 2 KA FIX — return value ab teen-tarfa hai:
         list  → kaamyab (khali list = is account ka waqai koi data nahi)
         None  → FAIL (job failed / timeout / exception)

       v2.1 dono soorat mein `[]` deta tha, isliye caller ko pata hi nahi
       chalta tha ke account fail hua ya khali tha — aur global DELETE us
       account ka data uda deta tha.

    🛡️ BUG 3 KA FIX — polling ab bounded hai:
       v2.1: `while status != "Job Completed"` — koi limit nahi.
       08-21 ka run 80% pe atka raha, 1 ghante baad GitHub ne maar diya.
       Ab MAX_POLL_SECONDS (default 900s) ke baad None return hota hai.
    """
    start, end = date_range()
    fields = INSIGHT_FIELDS[:]
    if extra_fields:
        fields = list(set(fields + extra_fields))

    params = {
        "level":          level,
        "time_range":     {"since": start, "until": end},
        "time_increment": 1,
        "limit":          500,
    }
    if breakdowns:
        params["breakdowns"] = breakdowns
    if params_extra:
        params.update(params_extra)

    acct_label = account.get_id()

    for attempt in range(3):
        try:
            async_job = account.get_insights(fields=fields, params=params, is_async=True)
            async_job = async_job.api_get()

            waited = 0
            last_pct = -1
            stall = 0
            while True:
                status = async_job[AdReportRun.Field.async_status]
                if status == "Job Completed":
                    break
                if status in ("Job Failed", "Job Skipped"):
                    log.warning(f"    Async job {status} ({acct_label})")
                    return None                      # 🛡️ fail — [] NAHI

                if waited >= MAX_POLL_SECONDS:
                    log.warning(f"    ⏱️  Timeout {MAX_POLL_SECONDS}s pe "
                                f"({last_pct}% pe atka, {acct_label})")
                    return None                      # 🛡️ timeout — [] NAHI

                time.sleep(POLL_INTERVAL)
                waited += POLL_INTERVAL
                async_job = async_job.api_get()
                pct = async_job.get(AdReportRun.Field.async_percent_completion, 0)

                # progress ruk gaya to har poll pe log mat bharo
                if pct != last_pct:
                    log.info(f"    Job status: {async_job[AdReportRun.Field.async_status]} "
                             f"({pct}%) — {waited}s")
                    last_pct, stall = pct, 0
                else:
                    stall += 1
                    if stall % 6 == 0:
                        log.info(f"    ...abhi bhi {pct}% pe ({waited}s / {MAX_POLL_SECONDS}s)")

            results = []
            cursor = async_job.get_result(params={"limit": 500})
            for row in cursor:
                results.append(row)
            log.info(f"    Got {len(results):,} rows ({acct_label})")
            return results

        except Exception as e:
            err_str = str(e)
            if any(w in err_str.lower() for w in ("rate", "too many", "limit")):
                wait = 60 * (attempt + 1)
                log.warning(f"    Rate limit — {wait}s wait, retry {attempt+1}/3 ({acct_label})")
                time.sleep(wait)
            else:
                log.warning(f"    Insights error ({acct_label}): {e}")
                return None                          # 🛡️ fail — [] NAHI

    log.warning(f"    3 retries ke baad haar gaye ({acct_label})")
    return None                                      # 🛡️ fail — [] NAHI


def _run_per_account(accounts, fn, label):
    """
    🆕 v3 — har fetch isi se guzarta hai.

    Returns (rows, ok_accounts):
        ok_accounts = sirf wo accounts jinka fetch KAAMYAB raha.
        Jis account ka fetch None de, wo ok_accounts mein NAHI aata —
        aur load_to_bq uska purana data bilkul nahi chhuta.
    """
    rows, ok = [], set()
    for account in accounts:
        aid = norm_acct(account.get_id())
        try:
            out = fn(account)
        except Exception as e:
            record_failure(f"{label}[{aid}]", e)
            continue

        if out is None:
            record_failure(f"{label}[{aid}]", "fetch fail — is account ka data CHHUA NAHI jayega")
            continue

        rows.extend(out)
        ok.add(aid)
    log.info(f"  {label}: {len(rows):,} rows from {len(ok)}/{len(accounts)} accounts")
    return rows, ok


# ─── FETCH FUNCTIONS ──────────────────────────────────────────────────────────
def fetch_account_daily(accounts):
    log.info("Fetching Account Daily...")
    def one(account):
        ins = get_insights_async(account, level="account")
        if ins is None: return None
        return [{
            "date_start":   i.get("date_start"),
            "account_id":   i.get("account_id"),
            "account_name": i.get("account_name"),
            **build_kpi(i),
            "_ingested_at": now_ts(),
        } for i in ins]
    return _run_per_account(accounts, one, "account_daily")


def fetch_campaign_daily_insights(accounts):
    log.info("Fetching Campaign Daily Insights...")
    def one(account):
        ins = get_insights_async(account, level="campaign")
        if ins is None: return None
        return [{
            "date_start":    i.get("date_start"),
            "account_id":    i.get("account_id"),
            "campaign_id":   i.get("campaign_id"),
            "campaign_name": i.get("campaign_name"),
            "objective":     i.get("objective"),
            "buying_type":   i.get("buying_type"),
            **build_kpi(i),
            "_ingested_at":  now_ts(),
        } for i in ins]
    return _run_per_account(accounts, one, "campaign_daily_insights")


def fetch_adset_daily_insights(accounts):
    log.info("Fetching Adset Daily Insights...")
    def one(account):
        ins = get_insights_async(account, level="adset")
        if ins is None: return None
        return [{
            "date_start":    i.get("date_start"),
            "account_id":    i.get("account_id"),
            "campaign_id":   i.get("campaign_id"),
            "campaign_name": i.get("campaign_name"),
            "adset_id":      i.get("adset_id"),
            "adset_name":    i.get("adset_name"),
            "objective":     i.get("objective"),
            "buying_type":   i.get("buying_type"),
            **build_kpi(i),
            "_ingested_at":  now_ts(),
        } for i in ins]
    return _run_per_account(accounts, one, "adset_daily_insights")


def fetch_ad_insights_daily(accounts):
    log.info("Fetching Ad Insights Daily...")
    def one(account):
        ins = get_insights_async(account, level="ad")
        if ins is None: return None
        return [{
            "date_start":    i.get("date_start"),
            "date_stop":     i.get("date_stop"),
            "account_id":    i.get("account_id"),
            "account_name":  i.get("account_name"),
            "campaign_id":   i.get("campaign_id"),
            "campaign_name": i.get("campaign_name"),
            "adset_id":      i.get("adset_id"),
            "adset_name":    i.get("adset_name"),
            "ad_id":         i.get("ad_id"),
            "ad_name":       i.get("ad_name"),
            "objective":     i.get("objective"),
            "buying_type":   i.get("buying_type"),
            **build_kpi(i),
            "_ingested_at":  now_ts(),
        } for i in ins]
    return _run_per_account(accounts, one, "ad_insights_daily")


def fetch_breakdown(accounts, level, breakdowns, extra_keys, label):
    log.info(f"Fetching breakdown: {breakdowns}...")
    def one(account):
        ins = get_insights_async(account, level=level, breakdowns=breakdowns)
        if ins is None: return None
        out = []
        for i in ins:
            row = {
                "date_start":  i.get("date_start"),
                "account_id":  i.get("account_id"),
                "campaign_id": i.get("campaign_id"),
                "adset_id":    i.get("adset_id"),
                "ad_id":       i.get("ad_id"),
            }
            for key in extra_keys:
                row[key] = i.get(key)
            row.update(build_kpi(i))
            row["_ingested_at"] = now_ts()
            out.append(row)
        return out
    return _run_per_account(accounts, one, label)


def fetch_ad_delivery(accounts):
    log.info("Fetching Ad Delivery (quality rankings)...")
    def one(account):
        ins = get_insights_async(account, level="ad")
        if ins is None: return None
        out = []
        for i in ins:
            qr, er, cr = (i.get("quality_ranking"), i.get("engagement_rate_ranking"),
                          i.get("conversion_rate_ranking"))
            if any([qr, er, cr]):
                out.append({
                    "date_start":               i.get("date_start"),
                    "account_id":               i.get("account_id"),
                    "campaign_id":              i.get("campaign_id"),
                    "campaign_name":            i.get("campaign_name"),
                    "adset_id":                 i.get("adset_id"),
                    "adset_name":               i.get("adset_name"),
                    "ad_id":                    i.get("ad_id"),
                    "ad_name":                  i.get("ad_name"),
                    "quality_ranking":          qr,
                    "engagement_rate_ranking":  er,
                    "conversion_rate_ranking":  cr,
                    "impressions":              safe_int(i.get("impressions")),
                    "spend":                    safe_float(i.get("spend")),
                    "_ingested_at":             now_ts(),
                })
        return out
    return _run_per_account(accounts, one, "ad_delivery")


def fetch_reach_frequency(accounts):
    log.info("Fetching Reach & Frequency...")
    def one(account):
        ins = get_insights_async(account, level="adset", extra_fields=[
            AdsInsights.Field.campaign_name, AdsInsights.Field.adset_name])
        if ins is None: return None
        return [{
            "date_start":    i.get("date_start"),
            "account_id":    i.get("account_id"),
            "campaign_id":   i.get("campaign_id"),
            "campaign_name": i.get("campaign_name"),
            "adset_id":      i.get("adset_id"),
            "adset_name":    i.get("adset_name"),
            "reach":         safe_int(i.get("reach")),
            "frequency":     safe_float(i.get("frequency")),
            "impressions":   safe_int(i.get("impressions")),
            "spend":         safe_float(i.get("spend")),
            "cpp":           safe_float(i.get("cpp")),
            "_ingested_at":  now_ts(),
        } for i in ins]
    return _run_per_account(accounts, one, "reach_frequency")


def fetch_app_events(accounts):
    log.info("Fetching App Events...")
    def one(account):
        ins = get_insights_async(account, level="account")
        if ins is None: return None
        out = []
        for i in ins:
            for action in i.get("actions", []):
                at = action.get("action_type", "")
                if "app" in at or "mobile" in at:
                    out.append({
                        "date":         i.get("date_start"),
                        "account_id":   i.get("account_id"),
                        "app_id":       "",
                        "event_name":   at,
                        "count":        safe_int(action.get("value")),
                        "unique_users": None,
                        "_ingested_at": now_ts(),
                    })
        return out
    return _run_per_account(accounts, one, "app_events")


def fetch_pixel_events(accounts):
    log.info("Fetching Pixel Events...")
    def one(account):
        ins = get_insights_async(account, level="account")
        if ins is None: return None
        return [{
            "date":         i.get("date_start"),
            "account_id":   i.get("account_id"),
            "event_name":   a.get("action_type"),
            "count":        safe_int(a.get("value")),
            "_ingested_at": now_ts(),
        } for i in ins for a in i.get("actions", [])]
    return _run_per_account(accounts, one, "pixel_events")

# ─── STRUCTURE FETCHERS (campaigns / adsets / ads / creatives / audiences) ────
def fetch_with_retry(fn, max_retries=5):
    """Rate-limit pe exponential backoff. Fail pe raise karta hai (None nahi)."""
    for attempt in range(max_retries):
        try:
            return list(fn())
        except Exception as e:
            err_str = str(e).lower()
            if any(w in err_str for w in ("rate", "too many", "limit reached")) or "2446079" in str(e):
                wait = 120 * (attempt + 1)
                log.warning(f"  Rate limit — {wait}s wait, retry {attempt+1}/{max_retries}...")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"{max_retries} retries ke baad haar gaye")


def fetch_campaigns(accounts):
    log.info("Fetching Campaigns...")
    fields = [
        Campaign.Field.id, Campaign.Field.name,
        Campaign.Field.status, Campaign.Field.effective_status,
        Campaign.Field.objective, Campaign.Field.buying_type,
        Campaign.Field.bid_strategy, Campaign.Field.daily_budget,
        Campaign.Field.lifetime_budget, Campaign.Field.budget_remaining,
        Campaign.Field.spend_cap, Campaign.Field.start_time,
        Campaign.Field.stop_time, Campaign.Field.created_time,
        Campaign.Field.updated_time,
    ]
    def one(account):
        time.sleep(2)
        campaigns = fetch_with_retry(
            lambda: account.get_campaigns(fields=fields, params={"limit": 200}))
        return [{
            "account_id":       account.get_id(),
            "campaign_id":      c.get("id"),
            "name":             c.get("name"),
            "status":           c.get("status"),
            "effective_status": c.get("effective_status"),
            "objective":        c.get("objective"),
            "buying_type":      c.get("buying_type"),
            "bid_strategy":     c.get("bid_strategy"),
            "daily_budget":     safe_float(c.get("daily_budget")),
            "lifetime_budget":  safe_float(c.get("lifetime_budget")),
            "budget_remaining": safe_float(c.get("budget_remaining")),
            "spend_cap":        safe_float(c.get("spend_cap")),
            "start_time":       parse_ts(c.get("start_time")),
            "stop_time":        parse_ts(c.get("stop_time")),
            "created_time":     parse_ts(c.get("created_time")),
            "updated_time":     parse_ts(c.get("updated_time")),
            "_ingested_at":     now_ts(),
        } for c in campaigns]
    return _run_per_account(accounts, one, "campaigns")


def fetch_adsets_for_account(account_id):
    """
    🛡️ v3: fail pe None (v2.1 adhoori list de deta tha, jo TRUNCATE ke saath
       mil kar dimension rows uda deta tha).
    """
    fields = ("id,campaign_id,name,status,effective_status,optimization_goal,billing_event,"
              "bid_strategy,bid_amount,daily_budget,lifetime_budget,targeting,"
              "promoted_object{application_id,object_store_url,pixel_id,custom_event_type},"
              "start_time,end_time,created_time,updated_time")
    url = f"https://graph.facebook.com/v18.0/{account_id}/adsets"
    all_adsets = []
    first_params = {"fields": fields, "limit": 50, "access_token": FB_ACCESS_TOKEN}
    page = 0
    while url and page < 200:
        page += 1
        succeeded = False
        for attempt in range(5):
            try:
                resp = requests.get(
                    url,
                    params=first_params if page == 1 else {"access_token": FB_ACCESS_TOKEN},
                    timeout=90,
                ).json()
                if "error" in resp:
                    err = resp["error"]
                    if err.get("code") in (17, 80000) or "rate" in str(err).lower() or "2446079" in str(err):
                        wait = 120 * (attempt + 1)
                        log.warning(f"  Rate limit on adsets page {page} — {wait}s wait...")
                        time.sleep(wait)
                        continue
                    log.warning(f"  Adset API error: {err}")
                    return None                       # 🛡️ adhoori list NAHI
                all_adsets.extend(resp.get("data", []))
                log.info(f"  Got {len(resp.get('data', []))} adsets "
                         f"(page {page}, total {len(all_adsets)})")
                url = resp.get("paging", {}).get("next")
                time.sleep(3)
                succeeded = True
                break
            except Exception as e:
                log.warning(f"  Adset fetch error page {page}: {e}")
                return None                           # 🛡️ adhoori list NAHI
        if not succeeded:
            log.warning(f"  Adsets: page {page} pe 5 retries ke baad haar gaye")
            return None                               # 🛡️ adhoori list NAHI
    return all_adsets


def fetch_adsets(accounts):
    log.info("Fetching Ad Sets...")
    def one(account):
        time.sleep(30)
        log.info(f"  Fetching adsets for {account.get_id()}...")
        adsets = fetch_adsets_for_account(account.get_id())
        if adsets is None:
            return None
        out = []
        for s in adsets:
            t   = s.get("targeting") or {}
            geo = t.get("geo_locations") or {}
            po  = s.get("promoted_object") or {}
            store_url = po.get("object_store_url")
            android_pkg, apple_store_id = extract_package_from_store_url(store_url)
            out.append({
                "account_id":                   account.get_id(),
                "adset_id":                     s.get("id"),
                "campaign_id":                  s.get("campaign_id"),
                "name":                         s.get("name"),
                "status":                       s.get("status"),
                "effective_status":             s.get("effective_status"),
                "optimization_goal":            s.get("optimization_goal"),
                "billing_event":                s.get("billing_event"),
                "bid_strategy":                 s.get("bid_strategy"),
                "bid_amount":                   safe_float(s.get("bid_amount")),
                "daily_budget":                 safe_float(s.get("daily_budget")),
                "lifetime_budget":              safe_float(s.get("lifetime_budget")),
                "targeting_countries":          ",".join(geo.get("countries", [])),
                "targeting_age_min":            safe_int(t.get("age_min")),
                "targeting_age_max":            safe_int(t.get("age_max")),
                "targeting_genders":            json.dumps(t.get("genders", [])),
                "targeting_custom_audiences":   json.dumps([a.get("id") for a in t.get("custom_audiences", [])]),
                "placements_publisher_platforms": json.dumps(t.get("publisher_platforms", [])),
                "promoted_object_app_id":         po.get("application_id"),
                "promoted_object_pixel_id":       po.get("pixel_id"),
                "promoted_object_object_store_url":  store_url,
                "promoted_object_android_package":   android_pkg,
                "promoted_object_apple_app_store_id": apple_store_id,
                "start_time":                   parse_ts(s.get("start_time")),
                "end_time":                     parse_ts(s.get("end_time")),
                "created_time":                 parse_ts(s.get("created_time")),
                "updated_time":                 parse_ts(s.get("updated_time")),
                "_ingested_at":                 now_ts(),
            })
        return out
    return _run_per_account(accounts, one, "adsets")


def fetch_ads(accounts):
    log.info("Fetching Ads...")
    fields = [
        Ad.Field.id, Ad.Field.adset_id, Ad.Field.campaign_id,
        Ad.Field.name, Ad.Field.status, Ad.Field.effective_status,
        Ad.Field.creative, Ad.Field.created_time, Ad.Field.updated_time,
    ]
    def one(account):
        time.sleep(2)
        ads = fetch_with_retry(lambda: account.get_ads(fields=fields, params={"limit": 200}))
        out = []
        for a in ads:
            cr  = a.get("creative") or {}
            oss = cr.get("object_story_spec") or {}
            ld  = oss.get("link_data") or {}
            out.append({
                "account_id":               account.get_id(),
                "ad_id":                    a.get("id"),
                "adset_id":                 a.get("adset_id"),
                "campaign_id":              a.get("campaign_id"),
                "name":                     a.get("name"),
                "status":                   a.get("status"),
                "effective_status":         a.get("effective_status"),
                "creative_id":              cr.get("id"),
                "creative_title":           cr.get("title") or cr.get("name"),
                "creative_body":            cr.get("body") or ld.get("message"),
                "creative_call_to_action":  (ld.get("call_to_action") or {}).get("type"),
                "created_time":             parse_ts(a.get("created_time")),
                "updated_time":             parse_ts(a.get("updated_time")),
                "_ingested_at":             now_ts(),
            })
        return out
    return _run_per_account(accounts, one, "ads")


def fetch_ad_creatives_for_account(account_id):
    """
    🆕 v3.2 — ad_creatives ab REST + apni pagination + retry se aata hai.

    🛡️ BUG 8 KA FIX
       v2.1 aur v3.1 dono mein `account.get_ad_creatives(...)` seedha SDK cursor
       tha — na retry, na page-size control. Teen run mein teen baar toota:
           run#183  act_1242798440606579  code 80004 (rate limit)
           run#183  act_1737594613510482  code 1 "reduce the amount of data"
           run#185  act_742591034622263   code 1/99 "unknown error"
       Har baar pagination ke BEECH mein (cursor `after=...` ke saath), yani
       aadhi list aa chuki hoti thi.

       Ab:
         · PAGE 25 (100 nahi) — "reduce the amount of data" isi se aata tha
         · har page pe 5 retry + exponential backoff
         · fail pe None (adhoori list NAHI) → us account ka purana data mehfooz
    """
    PAGE = int(os.environ.get("CREATIVES_PAGE_SIZE", "25"))
    fields = ("id,name,title,body,call_to_action_type,image_url,thumbnail_url,"
              "video_id,link_url,effective_object_story_id")
    url = f"https://graph.facebook.com/v18.0/{account_id}/adcreatives"
    out = []
    first_params = {"fields": fields, "limit": PAGE, "access_token": FB_ACCESS_TOKEN}
    page = 0
    while url and page < 500:
        page += 1
        ok = False
        for attempt in range(5):
            try:
                resp = requests.get(
                    url,
                    params=first_params if page == 1 else {"access_token": FB_ACCESS_TOKEN},
                    timeout=90,
                ).json()
                if "error" in resp:
                    err  = resp["error"]
                    code = err.get("code")
                    sub  = err.get("error_subcode")
                    # rate limit ya "too much data" → wait karke phir se
                    if code in (1, 2, 4, 17, 80000, 80004) or sub in (99, 2446079):
                        wait = 60 * (attempt + 1)
                        log.warning(f"  creatives page {page} — code={code}/{sub}, "
                                    f"{wait}s wait, retry {attempt+1}/5 ({account_id})")
                        time.sleep(wait)
                        continue
                    log.warning(f"  Creatives API error ({account_id}): {err}")
                    return None                      # 🛡️ adhoori list NAHI
                out.extend(resp.get("data", []))
                url = resp.get("paging", {}).get("next")
                time.sleep(2)
                ok = True
                break
            except Exception as e:
                log.warning(f"  Creatives fetch error page {page} ({account_id}): {e}")
                time.sleep(30)
        if not ok:
            log.warning(f"  Creatives: page {page} pe 5 retries ke baad haar gaye ({account_id})")
            return None                              # 🛡️ adhoori list NAHI
    log.info(f"  Got {len(out)} creatives ({account_id}, {page} pages)")
    return out


def fetch_ad_creatives(accounts):
    log.info("Fetching Ad Creatives...")
    def one(account):
        time.sleep(5)
        creatives = fetch_ad_creatives_for_account(account.get_id())
        if creatives is None:
            return None
        return [{
            "account_id":                account.get_id(),
            "creative_id":               c.get("id"),
            "name":                      c.get("name"),
            "title":                     c.get("title"),
            "body":                      c.get("body"),
            "call_to_action_type":       c.get("call_to_action_type"),
            "image_url":                 c.get("image_url"),
            "thumbnail_url":             c.get("thumbnail_url"),
            "video_id":                  c.get("video_id"),
            "link_url":                  c.get("link_url"),
            "effective_object_story_id": c.get("effective_object_story_id"),
            "_ingested_at":              now_ts(),
        } for c in creatives]
    return _run_per_account(accounts, one, "ad_creatives")


def fetch_auction_insights(accounts):
    log.info("Fetching Auction Insights...")
    start, end = date_range()
    def one(account):
        resp = requests.get(
            f"https://graph.facebook.com/v18.0/{account.get_id()}/insights",
            params={
                "level":          "adset",
                "time_range":     json.dumps({"since": start, "until": end}),
                "time_increment": 1,
                "fields":         "date_start,campaign_id,campaign_name,adset_id,adset_name,account_id",
                "limit":          500,
                "access_token":   FB_ACCESS_TOKEN,
            }, timeout=90,
        ).json()
        if "error" in resp:
            log.warning(f"  Auction insights error: {resp['error']}")
            return None
        return [{
            "date_start":          i.get("date_start"),
            "account_id":          i.get("account_id"),
            "campaign_id":         i.get("campaign_id"),
            "campaign_name":       i.get("campaign_name"),
            "adset_id":            i.get("adset_id"),
            "adset_name":          i.get("adset_name"),
            "impression_share":    None,
            "outranking_share":    None,
            "overlap_rate":        None,
            "position_above_rate": None,
            "_ingested_at":        now_ts(),
        } for i in resp.get("data", [])]
    return _run_per_account(accounts, one, "auction_insights")


def fetch_custom_audiences(accounts):
    log.info("Fetching Custom Audiences...")
    fields = [
        CustomAudience.Field.id, CustomAudience.Field.name,
        CustomAudience.Field.subtype, CustomAudience.Field.approximate_count_lower_bound,
        CustomAudience.Field.data_source, CustomAudience.Field.lookalike_spec,
        CustomAudience.Field.retention_days, CustomAudience.Field.time_created,
    ]
    def one(account):
        out = []
        for a in account.get_custom_audiences(fields=fields, params={"limit": 200}):
            try:
                ds = a.get("data_source")
                data_source_str = json.dumps(
                    ds.export_all_data() if hasattr(ds, "export_all_data") else (ds or {}))
            except Exception:
                data_source_str = str(a.get("data_source", ""))
            out.append({
                "account_id":        account.get_id(),
                "audience_id":       a.get("id"),
                "name":              a.get("name"),
                "subtype":           str(a.get("subtype", "")),
                "approximate_count": safe_int(a.get("approximate_count_lower_bound")),
                "data_source":       data_source_str,
                "lookalike_spec":    json.dumps(a.get("lookalike_spec") or {}),
                "retention_days":    safe_int(a.get("retention_days")),
                "created_time":      parse_ts(str(a.get("time_created", ""))) if a.get("time_created") else None,
                "_ingested_at":      now_ts(),
            })
        return out
    return _run_per_account(accounts, one, "custom_audiences")


def fetch_page_insights():
    """Page insights account-scoped nahi — purana behaviour."""
    log.info("Fetching Page Insights...")
    if not FB_PAGE_ID:
        log.info("  No FB_PAGE_ID set, skipping")
        return [], None

    try:
        resp = requests.get(
            f"https://graph.facebook.com/v18.0/{FB_PAGE_ID}",
            params={"fields": "access_token", "access_token": FB_ACCESS_TOKEN}, timeout=60
        ).json()
        page_token = resp.get("access_token", FB_ACCESS_TOKEN)
    except Exception as e:
        log.warning(f"  Could not get page token: {e}")
        page_token = FB_ACCESS_TOKEN

    metrics = ["page_impressions_unique", "page_post_engagements", "page_views_total",
               "page_video_views", "page_video_views_unique", "page_total_actions"]
    start, end = date_range()
    rows = []

    FacebookAdsApi.init(FB_APP_ID, FB_APP_SECRET, page_token, api_version="v18.0")
    for metric in metrics:
        try:
            page = Page(FB_PAGE_ID)
            for m in page.get_insights(params={
                    "metric": metric, "period": "day", "since": start, "until": end}):
                for entry in m.get("values", []):
                    val = entry.get("value")
                    rows.append({
                        "date":         entry.get("end_time", "")[:10],
                        "page_id":      FB_PAGE_ID,
                        "metric_name":  m.get("name"),
                        "value":        sum(val.values()) if isinstance(val, dict) else safe_float(val),
                        "period":       m.get("period"),
                        "_ingested_at": now_ts(),
                    })
        except Exception as e:
            log.warning(f"  Page metric {metric} error: {e}")

    FacebookAdsApi.init(FB_APP_ID, FB_APP_SECRET, FB_ACCESS_TOKEN, api_version="v18.0")
    log.info(f"  Fetched {len(rows)} page insight rows")
    return rows, None


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    log.info("🚀 Facebook → BigQuery sync v3.4")
    log.info(f"   Lookback: {LOOKBACK_DAYS}d | Business: {FB_BUSINESS_ID}")
    log.info(f"   MAX_POLL={MAX_POLL_SECONDS}s | ACTIVE_ONLY={ACTIVE_ONLY} | "
             f"ALLOW_TRUNCATE={ALLOW_TRUNCATE} | DRY_RUN={DRY_RUN}")

    FacebookAdsApi.init(app_id=FB_APP_ID, app_secret=FB_APP_SECRET,
                        access_token=FB_ACCESS_TOKEN, api_version="v18.0")

    accounts = get_all_ad_accounts()          # khali list pe khud exit(1) karta hai

    bq = get_bq_client()
    ensure_dataset(bq)
    for t in SCHEMAS:
        ensure_table(bq, t)

    log.info("── Account Level Insights ──")
    load_to_bq(bq, "account_daily",           fetch_account_daily(accounts))

    log.info("── Campaign Level Insights ──")
    load_to_bq(bq, "campaign_daily_insights", fetch_campaign_daily_insights(accounts))

    log.info("── Adset Level Insights ──")
    load_to_bq(bq, "adset_daily_insights",    fetch_adset_daily_insights(accounts))

    log.info("── Ad Level Insights ──")
    load_to_bq(bq, "ad_insights_daily",       fetch_ad_insights_daily(accounts))

    log.info("── Breakdown Insights ──")
    load_to_bq(bq, "ad_insights_by_country",
               fetch_breakdown(accounts, "ad", ["country"], ["country"],
                               "ad_insights_by_country"))
    load_to_bq(bq, "ad_insights_by_device",
               fetch_breakdown(accounts, "ad", ["device_platform", "impression_device"],
                               ["device_platform", "impression_device"],
                               "ad_insights_by_device"))
    load_to_bq(bq, "ad_insights_by_placement",
               fetch_breakdown(accounts, "ad",
                               ["publisher_platform", "platform_position", "impression_device"],
                               ["publisher_platform", "platform_position", "impression_device"],
                               "ad_insights_by_placement"))
    load_to_bq(bq, "ad_insights_by_age_gender",
               fetch_breakdown(accounts, "ad", ["age", "gender"], ["age", "gender"],
                               "ad_insights_by_age_gender"))

    log.info("── Campaign Structure ──")
    load_to_bq(bq, "campaigns",     fetch_campaigns(accounts))
    load_to_bq(bq, "adsets",        fetch_adsets(accounts))
    load_to_bq(bq, "ads",           fetch_ads(accounts))
    load_to_bq(bq, "ad_creatives",  fetch_ad_creatives(accounts))

    log.info("── Ad Delivery ──")
    load_to_bq(bq, "ad_delivery",   fetch_ad_delivery(accounts))

    log.info("── Additional Tables ──")
    load_to_bq(bq, "reach_frequency",  fetch_reach_frequency(accounts))
    load_to_bq(bq, "auction_insights", fetch_auction_insights(accounts))
    load_to_bq(bq, "app_events",       fetch_app_events(accounts))
    load_to_bq(bq, "pixel_events",     fetch_pixel_events(accounts))
    load_to_bq(bq, "page_insights",    fetch_page_insights())
    load_to_bq(bq, "custom_audiences", fetch_custom_audiences(accounts))

    # ── 🛡️ BUG 6 KA FIX: exit code ab sach bolta hai ────────────────────────
    if FAILURES:
        log.error("=" * 70)
        log.error(f"🔴 {len(FAILURES)} MASLE — run FAIL samjha jayega:")
        for f in FAILURES:
            log.error(f"   • {f}")
        log.error("=" * 70)
        log.error("Jin accounts ka fetch fail hua, unka purana data CHHUA NAHI gaya.")
        sys.exit(1)

    log.info("✅ Facebook sync v3.4 complete — 19 tables, koi masla nahi.")


if __name__ == "__main__":
    main()
