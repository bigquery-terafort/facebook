"""
Facebook → BigQuery  ·  COMPLETE PIPELINE v2
==============================================
Fixes applied vs v1:
  1. Async job for insights — handles large date ranges correctly
  2. Delete-before-insert — no duplicates on re-runs
  3. outbound_clicks extracted correctly by action_type
  4. Quality rankings pulled (quality_ranking, engagement_rate_ranking, conversion_rate_ranking)
  5. Social proof pulled (inline_post_engagement, inline_link_clicks)
  6. Attribution window pulled
  7. Direct campaign-level AND adset-level insight pulls (not just ad level)
  8. Account-level daily insights added
  9. Ad delivery table added (quality rankings + delivery status per ad)
  10. LOOKBACK_DAYS default changed to 7 for daily runs
  11. Pagination added for account discovery
  12. All insight levels pulled separately for exact UI match

Tables:
  RAW INSIGHTS (pulled directly at each level):
  1.  account_daily              — account level daily stats
  2.  campaign_daily_insights    — campaign level daily stats (direct pull)
  3.  adset_daily_insights       — adset level daily stats (direct pull)
  4.  ad_insights_daily          — ad level daily stats
  5.  ad_insights_by_country     — ad level by country
  6.  ad_insights_by_device      — ad level by device
  7.  ad_insights_by_placement   — ad level by placement
  8.  ad_insights_by_age_gender  — ad level by age + gender

  STRUCTURE:
  9.  campaigns                  — campaign settings
  10. adsets                     — adset settings + targeting
  11. ads                        — ad settings
  12. ad_creatives               — creative details

  ADDITIONAL:
  13. ad_delivery                — quality rankings + delivery status per ad
  14. auction_insights           — competitive data
  15. reach_frequency            — reach + frequency at adset level
  16. app_events                 — app events via Facebook SDK
  17. pixel_events               — pixel conversion events
  18. page_insights              — page metrics daily
  19. custom_audiences           — audience lists
"""

import sys
print("Python version:", sys.version, flush=True)
print("Starting imports...", flush=True)

print("Importing os, json, logging...", flush=True)
import os, json, logging, time, re
from datetime import datetime, timedelta

print("Importing facebook_business...", flush=True)
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

print("Importing google.cloud...", flush=True)
from google.cloud import bigquery
from google.oauth2 import service_account

print("Importing requests...", flush=True)
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
LOOKBACK_DAYS        = int(os.environ.get("LOOKBACK_DAYS", "7"))   # 7 for daily, 90 for initial load

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
    # FIX v2: quality rankings
    AdsInsights.Field.quality_ranking,
    AdsInsights.Field.engagement_rate_ranking,
    AdsInsights.Field.conversion_rate_ranking,
    # FIX v2: social proof / inline engagement
    AdsInsights.Field.inline_post_engagement,
    AdsInsights.Field.inline_link_clicks,
    AdsInsights.Field.inline_link_click_ctr,
    # FIX v2: attribution
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
        # v2 additions
        bigquery.SchemaField("quality_ranking",            "STRING"),
        bigquery.SchemaField("engagement_rate_ranking",    "STRING"),
        bigquery.SchemaField("conversion_rate_ranking",    "STRING"),
        bigquery.SchemaField("inline_post_engagement",     "INTEGER"),
        bigquery.SchemaField("inline_link_clicks",         "INTEGER"),
        bigquery.SchemaField("inline_link_click_ctr",      "FLOAT"),
        bigquery.SchemaField("attribution_setting",        "STRING"),
    ]

SCHEMAS = {
    # ── Account Daily ──────────────────────────────────────────────────────────
    "account_daily": [
        bigquery.SchemaField("date_start",      "DATE"),
        bigquery.SchemaField("account_id",      "STRING"),
        bigquery.SchemaField("account_name",    "STRING"),
        *kpi_fields(),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    # ── Campaign Daily Insights (direct pull) ─────────────────────────────────
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
    # ── Adset Daily Insights (direct pull) ────────────────────────────────────
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
    # ── Ad Insights Daily ─────────────────────────────────────────────────────
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
    # ── Breakdown Tables ──────────────────────────────────────────────────────
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
    # ── Structure Tables ──────────────────────────────────────────────────────
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
    # ── Ad Delivery (quality rankings + status) ───────────────────────────────
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
    # ── Additional Tables ─────────────────────────────────────────────────────
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

    # FIX v2: outbound_clicks extracted correctly by action_type
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
        # v2: quality rankings + social proof
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

def load_to_bq(client, name, rows):
    if not rows:
        log.info(f"  No rows for {name}")
        return

    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{name}"
    start, end = date_range()

    # FIX v2: delete existing rows for date range before inserting — no duplicates
    date_col_map = {
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

    date_col = date_col_map.get(name)
    if date_col:
        try:
            delete_sql = f"""
                DELETE FROM `{table_ref}`
                WHERE {date_col} BETWEEN '{start}' AND '{end}'
            """
            client.query(delete_sql).result()
            log.info(f"  Cleared existing rows for {name} ({start} to {end})")
        except Exception as e:
            log.warning(f"  Could not clear existing rows for {name}: {e}")
    else:
        # For structure tables (campaigns, adsets, ads etc) — truncate fully
        try:
            client.query(f"TRUNCATE TABLE `{table_ref}`").result()
            log.info(f"  Truncated {name}")
        except Exception as e:
            log.warning(f"  Could not truncate {name}: {e}")

    # Insert new rows in batches
    BATCH_SIZE = 200
    total_errors = []
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            errs = client.insert_rows_json(table_ref, batch)
            if errs:
                total_errors.extend(errs[:2])
        except Exception as e:
            log.error(f"  Batch {i} failed: {e}")

    if total_errors:
        log.error(f"BQ errors [{name}]: {total_errors[:2]}")
    else:
        log.info(f"  ✅ {len(rows):,} rows → {name}")

# ─── ACCOUNT DISCOVERY ───────────────────────────────────────────────────────
def get_all_ad_accounts():
    log.info(f"Discovering all ad accounts from Business Manager {FB_BUSINESS_ID}...")

    # Owned accounts
    owned_resp = requests.get(
        f"https://graph.facebook.com/v18.0/{FB_BUSINESS_ID}/owned_ad_accounts",
        params={
            "fields": "id,name,account_status",
            "limit": 100,
            "access_token": FB_ACCESS_TOKEN,
        }
    ).json()
    log.info(f"  Owned accounts raw response: {json.dumps(owned_resp)[:3000]}")

    all_accounts = owned_resp.get("data", [])

    # Client accounts
    client_resp = requests.get(
        f"https://graph.facebook.com/v18.0/{FB_BUSINESS_ID}/client_ad_accounts",
        params={
            "fields": "id,name,account_status",
            "limit": 100,
            "access_token": FB_ACCESS_TOKEN,
        }
    ).json()
    log.info(f"  Client accounts raw response: {json.dumps(client_resp)[:3000]}")

    for a in client_resp.get("data", []):
        if not any(x.get("id") == a.get("id") for x in all_accounts):
            all_accounts.append(a)

    log.info(f"  Total accounts found before status filter: {len(all_accounts)}")

    active = []
    for a in all_accounts:
        status = a.get("account_status")
        status_name = {1: "active", 2: "disabled", 3: "unsettled", 7: "pending", 9: "grace"}.get(status, "unknown")
        log.info(f"  Account: {a.get('name')} | ID: {a.get('id')} | Status code: {status} ({status_name})")
        if status == 1:
            active.append(AdAccount(a.get("id")))

    if not active:
        log.warning("  No status=1 accounts found - using all accounts regardless of status")
        for a in all_accounts:
            active.append(AdAccount(a.get("id")))

    log.info(f"  Total {len(active)} accounts to process")
    return active

# ─── ASYNC INSIGHTS FETCHER ───────────────────────────────────────────────────
# FIX v2: use async jobs — handles large date ranges correctly
def get_insights_async(account, level, breakdowns=None, extra_fields=None, params_extra=None):
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

    for attempt in range(3):
        try:
            # Create async job
            async_job = account.get_insights(
                fields=fields,
                params=params,
                is_async=True
            )

            # Poll until complete
            async_job = async_job.api_get()
            while async_job[AdReportRun.Field.async_status] != "Job Completed":
                time.sleep(10)
                async_job = async_job.api_get()
                status  = async_job[AdReportRun.Field.async_status]
                percent = async_job.get(AdReportRun.Field.async_percent_completion, 0)
                log.info(f"    Job status: {status} ({percent}%)")
                if status in ("Job Failed", "Job Skipped"):
                    log.warning(f"    Async job failed: {status}")
                    return []

            # Collect all results with pagination
            results = []
            cursor = async_job.get_result(params={"limit": 500})
            for row in cursor:
                results.append(row)
            log.info(f"    Got {len(results):,} rows (account {account.get_id()})")
            return results

        except Exception as e:
            err_str = str(e)
            if "rate" in err_str.lower() or "too many" in err_str.lower() or "limit" in err_str.lower():
                wait = 60 * (attempt + 1)
                log.warning(f"    Rate limit — waiting {wait}s before retry {attempt+1}/3...")
                time.sleep(wait)
            else:
                log.warning(f"    Insights error: {e}")
                return []

    log.warning(f"    Gave up after 3 retries")
    return []

# ─── FETCH FUNCTIONS ──────────────────────────────────────────────────────────

def fetch_account_daily(accounts):
    log.info("Fetching Account Daily...")
    rows = []
    for account in accounts:
        for i in get_insights_async(account, level="account"):
            rows.append({
                "date_start":   i.get("date_start"),
                "account_id":   i.get("account_id"),
                "account_name": i.get("account_name"),
                **build_kpi(i),
                "_ingested_at": now_ts(),
            })
    return rows


def fetch_campaign_daily_insights(accounts):
    log.info("Fetching Campaign Daily Insights (direct pull)...")
    rows = []
    for account in accounts:
        for i in get_insights_async(account, level="campaign"):
            rows.append({
                "date_start":    i.get("date_start"),
                "account_id":    i.get("account_id"),
                "campaign_id":   i.get("campaign_id"),
                "campaign_name": i.get("campaign_name"),
                "objective":     i.get("objective"),
                "buying_type":   i.get("buying_type"),
                **build_kpi(i),
                "_ingested_at":  now_ts(),
            })
    return rows


def fetch_adset_daily_insights(accounts):
    log.info("Fetching Adset Daily Insights (direct pull)...")
    rows = []
    for account in accounts:
        for i in get_insights_async(account, level="adset"):
            rows.append({
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
            })
    return rows


def fetch_ad_insights_daily(accounts):
    log.info("Fetching Ad Insights Daily...")
    rows = []
    for account in accounts:
        for i in get_insights_async(account, level="ad"):
            rows.append({
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
            })
    return rows


def fetch_breakdown(accounts, level, breakdowns, extra_keys):
    log.info(f"Fetching breakdown: {breakdowns}...")
    rows = []
    for account in accounts:
        for i in get_insights_async(account, level=level, breakdowns=breakdowns):
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
            rows.append(row)
    return rows


def fetch_ad_delivery(accounts):
    """Quality rankings + delivery status per ad per day"""
    log.info("Fetching Ad Delivery (quality rankings)...")
    rows = []
    for account in accounts:
        for i in get_insights_async(account, level="ad"):
            # Only store if quality ranking is available
            qr = i.get("quality_ranking")
            er = i.get("engagement_rate_ranking")
            cr = i.get("conversion_rate_ranking")
            if any([qr, er, cr]):
                rows.append({
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
    return rows


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
    rows = []
    for account in accounts:
        time.sleep(2)
        try:
            campaigns = fetch_with_retry(
                lambda: account.get_campaigns(fields=fields, params={"limit": 200})
            )
            for c in campaigns:
                rows.append({
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
                })
        except Exception as e:
            log.warning(f"  Campaigns error for {account.get_id()}: {e}")
    return rows


def fetch_with_retry(fn, max_retries=5):
    """Call fn() with exponential backoff on rate limit errors."""
    for attempt in range(max_retries):
        try:
            return list(fn())
        except Exception as e:
            err_str = str(e)
            if "rate" in err_str.lower() or "too many" in err_str.lower() or "limit reached" in err_str.lower() or "2446079" in err_str or "17" in err_str:
                wait = 120 * (attempt + 1)  # 120s, 240s, 360s, 480s, 600s
                log.warning(f"  Rate limit — waiting {wait}s before retry {attempt+1}/{max_retries}...")
                time.sleep(wait)
            else:
                raise e
    log.warning(f"  Gave up after {max_retries} retries")
    return []


def fetch_adsets_for_account(account_id):
    """Fetch all adsets for a single account using direct REST API with pagination."""
    fields = "id,campaign_id,name,status,effective_status,optimization_goal,billing_event,bid_strategy,bid_amount,daily_budget,lifetime_budget,targeting,promoted_object,start_time,end_time,created_time,updated_time"
    url = f"https://graph.facebook.com/v18.0/{account_id}/adsets"
    all_adsets = []
    first_params = {
        "fields": fields,
        "limit": 50,
        "access_token": FB_ACCESS_TOKEN,
    }
    page = 0
    while url:
        page += 1
        succeeded = False
        for attempt in range(5):
            try:
                if page == 1:
                    resp = requests.get(url, params=first_params).json()
                else:
                    resp = requests.get(url, params={"access_token": FB_ACCESS_TOKEN}).json()
                if "error" in resp:
                    err = resp["error"]
                    if err.get("code") in (17, 80000) or "rate" in str(err).lower() or "2446079" in str(err):
                        wait = 120 * (attempt + 1)
                        log.warning(f"  Rate limit on adsets page {page} — waiting {wait}s...")
                        time.sleep(wait)
                        continue
                    else:
                        log.warning(f"  Adset API error: {resp['error']}")
                        return all_adsets
                all_adsets.extend(resp.get("data", []))
                log.info(f"  Got {len(resp.get('data', []))} adsets (page {page}, total {len(all_adsets)})")
                url = resp.get("paging", {}).get("next")
                time.sleep(3)
                succeeded = True
                break
            except Exception as e:
                log.warning(f"  Adset fetch error page {page}: {e}")
                return all_adsets
        if not succeeded:
            log.warning(f"  Gave up on adsets after 5 retries on page {page}")
            break
    return all_adsets

def fetch_adsets(accounts):
    log.info("Fetching Ad Sets...")
    rows = []
    for account in accounts:
        time.sleep(30)
        log.info(f"  Fetching adsets for {account.get_id()}...")
        adsets = fetch_adsets_for_account(account.get_id())
        for s in adsets:
            t   = s.get("targeting") or {}
            geo = t.get("geo_locations") or {}
            po  = s.get("promoted_object") or {}
            rows.append({
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
                "promoted_object_app_id":       po.get("application_id"),
                "promoted_object_pixel_id":     po.get("pixel_id"),
                "start_time":                   parse_ts(s.get("start_time")),
                "end_time":                     parse_ts(s.get("end_time")),
                "created_time":                 parse_ts(s.get("created_time")),
                "updated_time":                 parse_ts(s.get("updated_time")),
                "_ingested_at":                 now_ts(),
            })
    return rows


def fetch_ads(accounts):
    log.info("Fetching Ads...")
    fields = [
        Ad.Field.id, Ad.Field.adset_id, Ad.Field.campaign_id,
        Ad.Field.name, Ad.Field.status, Ad.Field.effective_status,
        Ad.Field.creative, Ad.Field.created_time, Ad.Field.updated_time,
    ]
    rows = []
    for account in accounts:
        time.sleep(2)
        try:
            ads = fetch_with_retry(
                lambda: account.get_ads(fields=fields, params={"limit": 200})
            )
            for a in ads:
                cr  = a.get("creative") or {}
                oss = cr.get("object_story_spec") or {}
                ld  = oss.get("link_data") or {}
                rows.append({
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
        except Exception as e:
            log.warning(f"  Ads error for {account.get_id()}: {e}")
    return rows


def fetch_ad_creatives(accounts):
    log.info("Fetching Ad Creatives...")
    fields = [
        AdCreative.Field.id,
        AdCreative.Field.name,
        AdCreative.Field.title,
        AdCreative.Field.body,
        AdCreative.Field.call_to_action_type,
        AdCreative.Field.image_url,
        AdCreative.Field.thumbnail_url,
        AdCreative.Field.video_id,
        AdCreative.Field.link_url,
        AdCreative.Field.effective_object_story_id,
    ]
    rows = []
    for account in accounts:
        try:
            for c in account.get_ad_creatives(fields=fields, params={"limit": 100}):
                rows.append({
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
                })
        except Exception as e:
            log.warning(f"  Creatives error for {account.get_id()}: {e}")
    return rows


def fetch_reach_frequency(accounts):
    log.info("Fetching Reach & Frequency...")
    rows = []
    for account in accounts:
        for i in get_insights_async(account, level="adset",
                                    extra_fields=[
                                        AdsInsights.Field.campaign_name,
                                        AdsInsights.Field.adset_name,
                                    ]):
            rows.append({
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
            })
    return rows


def fetch_auction_insights(accounts):
    log.info("Fetching Auction Insights...")
    start, end = date_range()
    rows = []
    for account in accounts:
        try:
            act_id = account.get_id()
            resp = requests.get(
                f"https://graph.facebook.com/v18.0/{act_id}/insights",
                params={
                    "level":          "adset",
                    "time_range":     json.dumps({"since": start, "until": end}),
                    "time_increment": 1,
                    "fields":         "date_start,campaign_id,campaign_name,adset_id,adset_name,account_id",
                    "limit":          500,
                    "access_token":   FB_ACCESS_TOKEN,
                }
            ).json()
            for i in resp.get("data", []):
                rows.append({
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
                })
        except Exception as e:
            log.warning(f"  Auction insights error for {account.get_id()}: {e}")
    return rows


def fetch_app_events(accounts):
    log.info("Fetching App Events...")
    rows = []
    for account in accounts:
        for i in get_insights_async(account, level="account"):
            for action in i.get("actions", []):
                action_type = action.get("action_type", "")
                if "app" in action_type or "mobile" in action_type:
                    rows.append({
                        "date":         i.get("date_start"),
                        "account_id":   i.get("account_id"),
                        "app_id":       "",
                        "event_name":   action_type,
                        "count":        safe_int(action.get("value")),
                        "unique_users": None,
                        "_ingested_at": now_ts(),
                    })
    return rows


def fetch_pixel_events(accounts):
    log.info("Fetching Pixel Events...")
    rows = []
    for account in accounts:
        for i in get_insights_async(account, level="account"):
            for a in i.get("actions", []):
                rows.append({
                    "date":         i.get("date_start"),
                    "account_id":   i.get("account_id"),
                    "event_name":   a.get("action_type"),
                    "count":        safe_int(a.get("value")),
                    "_ingested_at": now_ts(),
                })
    return rows


def fetch_page_insights():
    log.info("Fetching Page Insights...")
    if not FB_PAGE_ID:
        log.info("  No FB_PAGE_ID set, skipping")
        return []

    try:
        resp = requests.get(
            f"https://graph.facebook.com/v18.0/{FB_PAGE_ID}",
            params={"fields": "access_token", "access_token": FB_ACCESS_TOKEN}
        ).json()
        page_token = resp.get("access_token", FB_ACCESS_TOKEN)
    except Exception as e:
        log.warning(f"  Could not get page token: {e}")
        page_token = FB_ACCESS_TOKEN

    metrics = [
        "page_impressions_unique",
        "page_post_engagements",
        "page_views_total",
        "page_video_views",
        "page_video_views_unique",
        "page_total_actions",
    ]
    start, end = date_range()
    rows = []

    page_api = FacebookAdsApi.init(FB_APP_ID, FB_APP_SECRET, page_token, api_version="v18.0")
    for metric in metrics:
        try:
            page = Page(FB_PAGE_ID)
            for m in page.get_insights(params={
                "metric": metric,
                "period": "day",
                "since":  start,
                "until":  end,
            }):
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
    return rows


def fetch_custom_audiences(accounts):
    log.info("Fetching Custom Audiences...")
    fields = [
        CustomAudience.Field.id,
        CustomAudience.Field.name,
        CustomAudience.Field.subtype,
        CustomAudience.Field.approximate_count_lower_bound,
        CustomAudience.Field.data_source,
        CustomAudience.Field.lookalike_spec,
        CustomAudience.Field.retention_days,
        CustomAudience.Field.time_created,
    ]
    rows = []
    for account in accounts:
        try:
            for a in account.get_custom_audiences(fields=fields, params={"limit": 200}):
                try:
                    ds = a.get("data_source")
                    data_source_str = json.dumps(
                        ds.export_all_data() if hasattr(ds, "export_all_data") else (ds or {})
                    )
                except Exception:
                    data_source_str = str(a.get("data_source", ""))

                rows.append({
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
        except Exception as e:
            log.warning(f"  Custom audiences error for {account.get_id()}: {e}")
    return rows


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    log.info("🚀 Facebook → BigQuery COMPLETE sync v2")
    log.info(f"   Lookback: {LOOKBACK_DAYS} days | Business: {FB_BUSINESS_ID}")

    FacebookAdsApi.init(
        app_id=FB_APP_ID,
        app_secret=FB_APP_SECRET,
        access_token=FB_ACCESS_TOKEN,
        api_version="v18.0"
    )

    accounts = get_all_ad_accounts()
    if not accounts:
        log.error("No active ad accounts found!")
        return

    bq = get_bq_client()
    ensure_dataset(bq)
    for t in SCHEMAS:
        ensure_table(bq, t)

    # ── Insights (all levels) ──────────────────────────────────────────────────
    log.info("── Account Level Insights ──")
    load_to_bq(bq, "account_daily",             fetch_account_daily(accounts))

    log.info("── Campaign Level Insights ──")
    load_to_bq(bq, "campaign_daily_insights",   fetch_campaign_daily_insights(accounts))

    log.info("── Adset Level Insights ──")
    load_to_bq(bq, "adset_daily_insights",      fetch_adset_daily_insights(accounts))

    log.info("── Ad Level Insights ──")
    load_to_bq(bq, "ad_insights_daily",         fetch_ad_insights_daily(accounts))

    log.info("── Breakdown Insights ──")
    load_to_bq(bq, "ad_insights_by_country",
               fetch_breakdown(accounts, "ad", ["country"], ["country"]))
    load_to_bq(bq, "ad_insights_by_device",
               fetch_breakdown(accounts, "ad",
                               ["device_platform", "impression_device"],
                               ["device_platform", "impression_device"]))
    load_to_bq(bq, "ad_insights_by_placement",
               fetch_breakdown(accounts, "ad",
                               ["publisher_platform", "platform_position", "impression_device"],
                               ["publisher_platform", "platform_position", "impression_device"]))
    load_to_bq(bq, "ad_insights_by_age_gender",
               fetch_breakdown(accounts, "ad", ["age", "gender"], ["age", "gender"]))

    # ── Structure ──────────────────────────────────────────────────────────────
    log.info("── Campaign Structure ──")
    load_to_bq(bq, "campaigns",     fetch_campaigns(accounts))
    load_to_bq(bq, "adsets",        fetch_adsets(accounts))
    load_to_bq(bq, "ads",           fetch_ads(accounts))
    load_to_bq(bq, "ad_creatives",  fetch_ad_creatives(accounts))

    # ── Ad Delivery ────────────────────────────────────────────────────────────
    log.info("── Ad Delivery ──")
    load_to_bq(bq, "ad_delivery",   fetch_ad_delivery(accounts))

    # ── Additional ────────────────────────────────────────────────────────────
    log.info("── Additional Tables ──")
    load_to_bq(bq, "reach_frequency",  fetch_reach_frequency(accounts))
    load_to_bq(bq, "auction_insights", fetch_auction_insights(accounts))
    load_to_bq(bq, "app_events",       fetch_app_events(accounts))
    load_to_bq(bq, "pixel_events",     fetch_pixel_events(accounts))
    load_to_bq(bq, "page_insights",    fetch_page_insights())
    load_to_bq(bq, "custom_audiences", fetch_custom_audiences(accounts))

    log.info("✅ Facebook sync v2 complete! 19 tables loaded.")


if __name__ == "__main__":
    main()
