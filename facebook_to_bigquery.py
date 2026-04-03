def get_all_ad_accounts():
    log.info(f"Discovering all ad accounts from Business Manager {FB_BUSINESS_ID}...")

    # Owned accounts
    resp = requests.get(
        f"https://graph.facebook.com/v18.0/{FB_BUSINESS_ID}/owned_ad_accounts",
        params={
            "fields": "id,name,account_status",
            "limit": 100,
            "access_token": FB_ACCESS_TOKEN,
        }
    ).json()

    log.info(f"  Owned accounts API response: {json.dumps(resp)[:2000]}")

    all_accounts = resp.get("data", [])

    # Client accounts
    client_resp = requests.get(
        f"https://graph.facebook.com/v18.0/{FB_BUSINESS_ID}/client_ad_accounts",
        params={
            "fields": "id,name,account_status",
            "limit": 100,
            "access_token": FB_ACCESS_TOKEN,
        }
    ).json()

    log.info(f"  Client accounts API response: {json.dumps(client_resp)[:2000]}")

    for a in client_resp.get("data", []):
        if not any(x.get("id") == a.get("id") for x in all_accounts):
            all_accounts.append(a)

    log.info(f"  All accounts raw: {json.dumps(all_accounts)[:2000]}")

    active = []
    for a in all_accounts:
        status = a.get("account_status")
        log.info(f"  Account: {a.get('name')} | ID: {a.get('id')} | Status: {status}")
        if status == 1:
            active.append(AdAccount(a.get("id")))

    # If still empty — include ALL accounts regardless of status for debugging
    if not active:
        log.warning("  No status=1 accounts found — trying all accounts regardless of status")
        for a in all_accounts:
            active.append(AdAccount(a.get("id")))

    log.info(f"  Total {len(active)} accounts to process")
    return active
