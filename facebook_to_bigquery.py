"""
Facebook → BigQuery  ·  COMPLETE PIPELINE
==========================================
Pulls everything from Facebook Marketing API + Page Insights

Auto-discovers all ad accounts from Business Manager

Tables:
  1.  ad_insights_daily          — spend, installs, ROAS per ad per day
  2.  ad_insights_by_country     — breakdown by country
  3.  ad_insights_by_device      — breakdown by device
  4.  ad_insights_by_placement   — breakdown by placement
  5.  ad_insights_by_age_gender  — breakdown by age + gender
  6.  campaigns                  — campaign structure + budgets
  7.  adsets                     — ad set structure + targeting
  8.  ads                        — individual ads + creatives
  9.  page_insights              — page metrics daily
  10. pixel_events               — conversion events
"""

import os, json, logging, time
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.business import Business
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.page import Page
from facebook_business.adobjects.adsinsights import AdsInsights
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

FB_APP_ID            = os.environ["FB_APP_ID"]
FB_APP_SECRET        = os.environ["FB_APP_SECRET"]
FB_ACCESS_TOKEN      = os.environ["FB_ACCESS_TOKEN"]
FB_BUSINESS_ID       = os.environ["FB_BUSINESS_ID"]
FB_PAGE_ID           = os.environ.get("FB_PAGE_ID", "")
FB_PIXEL_ID          = os.environ.get("FB_PIXEL_ID", "")
GCP_PROJECT          = os.environ["GCP_PROJECT"]
BQ_DATASET           = os.environ.get("BQ_DATASET", "facebook_data")
GCP_CREDENTIALS_JSON = os.environ["GCP_CREDENTIALS_JSON"]
LOOKBACK_DAYS        = int(os.environ.get("LOOKBACK_DAYS", "30"))

# ─── ACTION TYPES ─────────────────────────────────────────────────────────────
INSTALL_ACTIONS  = {"mobile_app_install", "app_install"}
PURCHASE_ACTIONS = {"offsite_conversion.fb_pixel_purchase", "purchase", "omni_purchase"}
LEAD_ACTIONS     = {"lead", "offsite_conversion.fb_pixel_lead"}
ROAS_ACTIONS     = {"omni_purchase", "offsite_conversion.fb_pixel_purchase", "purchase"}
ADD_CART_ACTIONS = {"add_to_cart", "offsite_conversion.fb_pixel_add_to_cart"}
CHECKOUT_ACTIONS = {"initiate_checkout", "offsite_conversion.fb_pixel_initiate_checkout"}
TRIAL_ACTIONS    = {"start_trial", "subscribe"}

INSIGHT_FIELDS = [
    AdsInsights.Field.date_start, AdsInsights.Field.date_stop,
    AdsInsights.Field.campaign_id, AdsInsights.Field.campaign_name,
    AdsInsights.Field.adset_id, AdsInsights.Field.adset_name,
    AdsInsights.Field.ad_id, AdsInsights.Field.ad_name,
    AdsInsights.Field.account_id, AdsInsights.Field.account_name,
    AdsInsights.Field.objective, AdsInsights.Field.buying_type,
    AdsInsights.Field.impressions, AdsInsights.Field.clicks,
    AdsInsights.Field.spend, AdsInsights.Field.reach,
    AdsInsights.Field.frequency, AdsInsights.Field.cpc,
    AdsInsights.Field.cpm, AdsInsights.Field.ctr,
    AdsInsights.Field.cpp, AdsInsights.Field.unique_clicks,
    AdsInsights.Field.unique_ctr, AdsInsights.Field.actions,
    AdsInsights.Field.action_values, AdsInsights.Field.cost_per_action_type,
    AdsInsights.Field.video_p25_watched_actions,
    AdsInsights.Field.video_p50_watched_actions,
    AdsInsights.Field.video_p75_watched_actions,
    AdsInsights.Field.video_p100_watched_actions,
    AdsInsights.Field.outbound_clicks,
    AdsInsights.Field.outbound_clicks_ctr,
]

# ─── BQ SCHEMAS ───────────────────────────────────────────────────────────────
def kpi_fields():
    return [
        bigquery.SchemaField("impressions",          "INTEGER"),
        bigquery.SchemaField("clicks",               "INTEGER"),
        bigquery.SchemaField("spend",                "FLOAT"),
        bigquery.SchemaField("reach",                "INTEGER"),
        bigquery.SchemaField("frequency",            "FLOAT"),
        bigquery.SchemaField("cpc",                  "FLOAT"),
        bigquery.SchemaField("cpm",                  "FLOAT"),
        bigquery.SchemaField("ctr",                  "FLOAT"),
        bigquery.SchemaField("cpp",                  "FLOAT"),
        bigquery.SchemaField("unique_clicks",        "INTEGER"),
        bigquery.SchemaField("unique_ctr",           "FLOAT"),
        bigquery.SchemaField("mobile_app_installs",  "INTEGER"),
        bigquery.SchemaField("cost_per_install",     "FLOAT"),
        bigquery.SchemaField("purchases",            "INTEGER"),
        bigquery.SchemaField("purchase_value",       "FLOAT"),
        bigquery.SchemaField("cost_per_purchase",    "FLOAT"),
        bigquery.SchemaField("roas",                 "FLOAT"),
        bigquery.SchemaField("leads",                "INTEGER"),
        bigquery.SchemaField("add_to_cart",          "INTEGER"),
        bigquery.SchemaField("initiate_checkout",    "INTEGER"),
        bigquery.SchemaField("trials_started",       "INTEGER"),
        bigquery.SchemaField("outbound_clicks",      "INTEGER"),
        bigquery.SchemaField("outbound_ctr",         "FLOAT"),
        bigquery.SchemaField("video_p25_views",      "INTEGER"),
        bigquery.SchemaField("video_p50_views",      "INTEGER"),
        bigquery.SchemaField("video_p75_views",      "INTEGER"),
        bigquery.SchemaField("video_p100_views",     "INTEGER"),
    ]

SCHEMAS = {
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
    "page_insights": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("page_id",         "STRING"),
        bigquery.SchemaField("metric_name",     "STRING"),
        bigquery.SchemaField("value",           "FLOAT"),
        bigquery.SchemaField("period",          "STRING"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "pixel_events": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("account_id",      "STRING"),
        bigquery.SchemaField("event_name",      "STRING"),
        bigquery.SchemaField("count",           "INTEGER"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def safe_float(v):
    try: return float(v) if v not in (None,"") else None
    except: return None

def safe_int(v):
    try: return int(float(v)) if v not in (None,"") else None
    except: return None

def now_ts(): return datetime.utcnow().isoformat()

def parse_ts(ts):
    """Convert Facebook timestamp to BigQuery format."""
    if not ts: return None
    # Remove timezone offset +0000 and replace T with space
    return ts.replace("T", " ").replace("+0000", "").strip()

def date_range():
    end   = datetime.utcnow().date()
    start = end - timedelta(days=LOOKBACK_DAYS)
    return str(start), str(end)

def extract_actions(insight, action_types):
    return sum((safe_int(a.get("value")) or 0)
               for a in insight.get("actions", [])
               if a.get("action_type") in action_types)

def extract_action_values(insight, action_types):
    return sum((safe_float(av.get("value")) or 0.0)
               for av in insight.get("action_values", [])
               if av.get("action_type") in action_types)

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
    return {
        "impressions":        safe_int(insight.get("impressions")),
        "clicks":             safe_int(insight.get("clicks")),
        "spend":              spend,
        "reach":              safe_int(insight.get("reach")),
        "frequency":          safe_float(insight.get("frequency")),
        "cpc":                safe_float(insight.get("cpc")),
        "cpm":                safe_float(insight.get("cpm")),
        "ctr":                safe_float(insight.get("ctr")),
        "cpp":                safe_float(insight.get("cpp")),
        "unique_clicks":      safe_int(insight.get("unique_clicks")),
        "unique_ctr":         safe_float(insight.get("unique_ctr")),
        "mobile_app_installs": installs,
        "cost_per_install":   round(spend/installs, 4) if installs else None,
        "purchases":          purchases,
        "purchase_value":     purch_val,
        "cost_per_purchase":  extract_cost_per_action(insight, PURCHASE_ACTIONS),
        "roas":               round(purch_val/spend, 4) if spend and purch_val else None,
        "leads":              extract_actions(insight, LEAD_ACTIONS),
        "add_to_cart":        extract_actions(insight, ADD_CART_ACTIONS),
        "initiate_checkout":  extract_actions(insight, CHECKOUT_ACTIONS),
        "trials_started":     extract_actions(insight, TRIAL_ACTIONS),
        "outbound_clicks":    safe_int((insight.get("outbound_clicks") or [{}])[0].get("value")),
        "outbound_ctr":       safe_float((insight.get("outbound_clicks_ctr") or [{}])[0].get("value")),
        "video_p25_views":    extract_video(insight, "video_p25_watched_actions"),
        "video_p50_views":    extract_video(insight, "video_p50_watched_actions"),
        "video_p75_views":    extract_video(insight, "video_p75_watched_actions"),
        "video_p100_views":   extract_video(insight, "video_p100_watched_actions"),
    }

# ─── BQ HELPERS ───────────────────────────────────────────────────────────────
def get_bq_client():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_CREDENTIALS_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return bigquery.Client(project=GCP_PROJECT, credentials=creds)

def ensure_dataset(client):
    try: client.get_dataset(BQ_DATASET)
    except Exception:
        log.info(f"Creating dataset {BQ_DATASET}")
        client.create_dataset(bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}"))

def ensure_table(client, name):
    ref = client.dataset(BQ_DATASET).table(name)
    try: client.get_table(ref)
    except Exception:
        log.info(f"Creating table {name}")
        client.create_table(bigquery.Table(ref, schema=SCHEMAS[name]))

def load_to_bq(client, name, rows):
    if not rows: log.info(f"  No rows for {name}"); return
    table_ref    = f"{GCP_PROJECT}.{BQ_DATASET}.{name}"
    BATCH_SIZE   = 200
    total_errors = []
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        try:
            errs = client.insert_rows_json(table_ref, batch)
            if errs: total_errors.extend(errs[:2])
        except Exception as e:
            log.error(f"  Batch {i} failed: {e}")
    if total_errors: log.error(f"BQ errors [{name}]: {total_errors[:2]}")
    else: log.info(f"  ✅ {len(rows):,} rows → {name}")

# ─── GET ALL AD ACCOUNTS FROM BUSINESS MANAGER ────────────────────────────────
def get_all_ad_accounts():
    log.info(f"Discovering all ad accounts from Business Manager {FB_BUSINESS_ID}...")
    business = Business(FB_BUSINESS_ID)
    accounts = business.get_owned_ad_accounts(fields=[
        AdAccount.Field.id,
        AdAccount.Field.name,
        AdAccount.Field.account_status,
    ])
    active = [a for a in accounts if a.get("account_status") == 1]
    log.info(f"  Found {len(active)} active ad accounts")
    for a in active:
        log.info(f"  → {a.get('name')} ({a.get('id')})")
    return active

# ─── INSIGHTS FETCHER ─────────────────────────────────────────────────────────
def get_insights(account, breakdowns, extra_keys, params_extra=None):
    start, end = date_range()
    params = {
        "level":          "ad",
        "time_range":     {"since": start, "until": end},
        "time_increment": 1,
        "limit":          500,
    }
    if breakdowns:
        params["breakdowns"] = breakdowns
    if params_extra:
        params.update(params_extra)
    try:
        return list(account.get_insights(fields=INSIGHT_FIELDS, params=params))
    except Exception as e:
        log.warning(f"  Insights error: {e}")
        return []

# ─── FETCH FUNCTIONS ──────────────────────────────────────────────────────────
def fetch_ad_insights_daily(accounts):
    log.info("Fetching Ad Insights Daily...")
    rows = []
    for account in accounts:
        for i in get_insights(account, None, []):
            rows.append({
                "date_start":   i.get("date_start"),
                "date_stop":    i.get("date_stop"),
                "account_id":   i.get("account_id"),
                "account_name": i.get("account_name"),
                "campaign_id":  i.get("campaign_id"),
                "campaign_name":i.get("campaign_name"),
                "adset_id":     i.get("adset_id"),
                "adset_name":   i.get("adset_name"),
                "ad_id":        i.get("ad_id"),
                "ad_name":      i.get("ad_name"),
                "objective":    i.get("objective"),
                "buying_type":  i.get("buying_type"),
                **build_kpi(i),
                "_ingested_at": now_ts(),
            })
    return rows

def fetch_breakdown(accounts, breakdowns, extra_keys, table_name):
    log.info(f"Fetching {table_name}...")
    rows = []
    for account in accounts:
        for i in get_insights(account, breakdowns, extra_keys):
            row = {
                "date_start":   i.get("date_start"),
                "account_id":   i.get("account_id"),
                "campaign_id":  i.get("campaign_id"),
                "adset_id":     i.get("adset_id"),
                "ad_id":        i.get("ad_id"),
            }
            for key in extra_keys:
                row[key] = i.get(key)
            row.update(build_kpi(i))
            row["_ingested_at"] = now_ts()
            rows.append(row)
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
        try:
            for c in account.get_campaigns(fields=fields, params={"limit": 500}):
                rows.append({
                    "account_id":       account.get("id"),
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
            log.warning(f"  Campaigns error for {account.get('id')}: {e}")
    return rows

def fetch_adsets(accounts):
    log.info("Fetching Ad Sets...")
    fields = [
        AdSet.Field.id, AdSet.Field.campaign_id, AdSet.Field.name,
        AdSet.Field.status, AdSet.Field.effective_status,
        AdSet.Field.optimization_goal, AdSet.Field.billing_event,
        AdSet.Field.bid_strategy, AdSet.Field.bid_amount,
        AdSet.Field.daily_budget, AdSet.Field.lifetime_budget,
        AdSet.Field.targeting, AdSet.Field.promoted_object,
        AdSet.Field.start_time, AdSet.Field.end_time,
        AdSet.Field.created_time, AdSet.Field.updated_time,
    ]
    rows = []
    for account in accounts:
        try:
            for s in account.get_ad_sets(fields=fields, params={"limit": 500}):
                t   = s.get("targeting") or {}
                geo = t.get("geo_locations") or {}
                po  = s.get("promoted_object") or {}
                rows.append({
                    "account_id":                   account.get("id"),
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
        except Exception as e:
            log.warning(f"  Adsets error for {account.get('id')}: {e}")
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
        try:
            for a in account.get_ads(fields=fields, params={"limit": 500}):
                cr  = a.get("creative") or {}
                oss = cr.get("object_story_spec") or {}
                ld  = oss.get("link_data") or {}
                rows.append({
                    "account_id":               account.get("id"),
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
            log.warning(f"  Ads error for {account.get('id')}: {e}")
    return rows

def fetch_page_insights():
    log.info("Fetching Page Insights...")
    if not FB_PAGE_ID:
        log.info("  No FB_PAGE_ID set, skipping")
        return []

    # Get Page Access Token from user token
    import requests as req
    try:
        resp = req.get(
            f"https://graph.facebook.com/v18.0/{FB_PAGE_ID}",
            params={"fields": "access_token", "access_token": FB_ACCESS_TOKEN}
        ).json()
        page_token = resp.get("access_token", FB_ACCESS_TOKEN)
        log.info(f"  Got page access token: {'✅' if page_token != FB_ACCESS_TOKEN else '⚠️ using user token'}")
    except Exception as e:
        log.warning(f"  Could not get page token: {e}")
        page_token = FB_ACCESS_TOKEN

    # Only valid metrics that work with page token
    metrics = [
        "page_impressions",
        "page_impressions_unique",
        "page_impressions_paid",
        "page_impressions_organic",
        "page_engaged_users",
        "page_post_engagements",
        "page_views_total",
        "page_fans",
        "page_fan_adds",
        "page_fan_removes",
        "page_video_views",
        "page_video_views_unique",
    ]
    start, end = date_range()
    rows = []

    # Use page access token for page insights
    from facebook_business.api import FacebookAdsApi
    original_api = FacebookAdsApi.get_default_api()
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

    # Restore original API
    FacebookAdsApi.init(FB_APP_ID, FB_APP_SECRET, FB_ACCESS_TOKEN, api_version="v18.0")
    log.info(f"  Fetched {len(rows)} page insight rows")
    return rows

def fetch_pixel_events(accounts):
    log.info("Fetching Pixel/Conversion Events...")
    start, end = date_range()
    rows = []
    for account in accounts:
        try:
            insights = account.get_insights(
                fields=["date_start", "actions", "account_id"],
                params={
                    "level":          "account",
                    "time_range":     {"since": start, "until": end},
                    "time_increment": 1,
                    "limit":          200,
                }
            )
            for i in insights:
                for a in i.get("actions", []):
                    rows.append({
                        "date":         i.get("date_start"),
                        "account_id":   i.get("account_id"),
                        "event_name":   a.get("action_type"),
                        "count":        safe_int(a.get("value")),
                        "_ingested_at": now_ts(),
                    })
        except Exception as e:
            log.warning(f"  Pixel events error for {account.get('id')}: {e}")
    return rows

# ─── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    log.info("🚀 Facebook → BigQuery COMPLETE sync")
    log.info(f"   Lookback: {LOOKBACK_DAYS} days | Business: {FB_BUSINESS_ID}")

    # Use stable API version to avoid appsecret_proof issues
    os.environ["FACEBOOK_API_VERSION"] = "v18.0"
    FacebookAdsApi.init(
        app_id=FB_APP_ID,
        app_secret=FB_APP_SECRET,
        access_token=FB_ACCESS_TOKEN,
        api_version="v18.0"
    )

    # Auto-discover all active ad accounts
    accounts = get_all_ad_accounts()
    if not accounts:
        log.error("No active ad accounts found!")
        return

    bq = get_bq_client()
    ensure_dataset(bq)
    for t in SCHEMAS:
        ensure_table(bq, t)

    # Ad Insights
    log.info("── Ad Insights ──")
    load_to_bq(bq, "ad_insights_daily",
        fetch_ad_insights_daily(accounts))
    load_to_bq(bq, "ad_insights_by_country",
        fetch_breakdown(accounts, ["country"], ["country"], "ad_insights_by_country"))
    load_to_bq(bq, "ad_insights_by_device",
        fetch_breakdown(accounts, ["device_platform", "impression_device"],
                       ["device_platform", "impression_device"], "ad_insights_by_device"))
    load_to_bq(bq, "ad_insights_by_placement",
        fetch_breakdown(accounts, ["publisher_platform", "platform_position", "impression_device"],
                       ["publisher_platform", "platform_position", "impression_device"],
                       "ad_insights_by_placement"))
    load_to_bq(bq, "ad_insights_by_age_gender",
        fetch_breakdown(accounts, ["age", "gender"], ["age", "gender"], "ad_insights_by_age_gender"))

    # Structure
    log.info("── Campaign Structure ──")
    load_to_bq(bq, "campaigns",    fetch_campaigns(accounts))
    load_to_bq(bq, "adsets",       fetch_adsets(accounts))
    load_to_bq(bq, "ads",          fetch_ads(accounts))

    # Page + Pixel
    log.info("── Page & Pixel ──")
    load_to_bq(bq, "page_insights", fetch_page_insights())
    load_to_bq(bq, "pixel_events",  fetch_pixel_events(accounts))

    log.info("✅ Facebook sync complete! 10 tables loaded.")

if __name__ == "__main__":
    main()
