{{ config(
    materialized='table',
    tags=['gold', 'daily'],
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["platform", "event_name", "standardized_conversion_type"]
) }}

WITH google_ads AS (
    SELECT
        id as unique_id,
        'Google Ads' as platform,
        date,
        -- Standard Date Parts
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,

        -- EVENT CYCLE DIMENSIONS
        -- Matches ads_campaign_performance exactly
        week as week_display,             -- e.g. "Week 10"
        week_number,                      -- e.g. 10 (Slicer Value)
        week_number_to_sort,              -- Sort Order
        weeks_left,                       -- Scorecard Value (e.g. "12")

        -- Dimensions
        event_name,
        event_edition,                    -- e.g. "AMWC 2025"

        account_id,
        account_name,                     -- Added for context
        campaign_id,
        campaign_name,
        campaign_status,

        -- Google doesn't have ad_group/ad level for this report
        CAST(NULL AS STRING) as ad_group_name,
        CAST(NULL AS STRING) as ad_name,

        -- Pricing / Rate Context
        cut_off_rate,                     -- e.g. "EB", "FP"
        cut_off_rate_sort_order,          -- e.g. 1, 2, 3

        -- Conversion Details
        conversion_action_name as conversion_name,
        conversion_category as conversion_raw_category,

        -- GOOGLE MAPPING LOGIC (Preserved)
        CASE
            -- We catch this specific historical tag BEFORE the general exclusion rule
            WHEN conversion_action_name = 'NOTWORKING-Do NOT USE' THEN 'PURCHASE'

            -- EXCLUDE THE DUPLICATE / JUNK TAGS
            WHEN conversion_action_name LIKE '%NOTWORKING%' THEN 'OTHER'
            WHEN conversion_action_name LIKE '%Do NOT USE%' THEN 'OTHER'

            -- FIX THE "FAKE" LEADS
            WHEN conversion_action_name = 'Registration' THEN 'INITIATE_CHECKOUT'

            -- SPECIFIC TICKET SALES (The "Good" Revenue)
            WHEN conversion_action_name LIKE '%Ticket%' THEN 'PURCHASE'

            -- ACTUAL LEADS (High Quality)

           -- DYNAMIC REGISTRATION LOGIC: Purchase if value exists OR category is PURCHASE

            WHEN conversion_action_name = 'Registrations' AND (conversion_value > 0 OR conversion_category = 'PURCHASE') THEN 'PURCHASE'
            WHEN conversion_action_name = 'Registrations' THEN 'LEAD'

            WHEN conversion_action_name LIKE '%Submit lead form%' THEN 'LEAD'
            WHEN conversion_action_name LIKE '%Newsletter%' THEN 'LEAD'
            WHEN conversion_action_name = 'Prospect' THEN 'LEAD'

            -- FALLBACK TO CATEGORIES (Standard Logic)
            WHEN conversion_category = 'PURCHASE' THEN 'PURCHASE'
            WHEN conversion_category = 'SUBMIT_LEAD_FORM' THEN 'LEAD'
            WHEN conversion_category = 'SIGNUP' THEN 'SIGNUP'
            WHEN conversion_category = 'PAGE_VIEW' THEN 'PAGE_VIEW'
            WHEN conversion_category = 'ENGAGEMENT' THEN 'ENGAGEMENT'
            WHEN conversion_category LIKE '%CART%' THEN 'ADD_TO_CART'
            WHEN conversion_category LIKE '%CHECKOUT%' THEN 'INITIATE_CHECKOUT'
            ELSE 'OTHER'
        END as standardized_conversion_type,

        -- METRICS (SMART ROUTING)
        -- Route secondary lead actions to 'all_conversions' so they aren't lost,
        -- while keeping standard 'conversions' for Purchases to protect ROAS.
        CASE
            WHEN conversion_action_name = 'Prospect' THEN all_conversions
            WHEN conversion_category = 'SUBMIT_LEAD_FORM' THEN all_conversions
            WHEN conversion_action_name = 'Registration' THEN all_conversions
            WHEN conversion_action_name LIKE '%Submit lead form%' THEN all_conversions
            WHEN conversion_action_name LIKE '%Newsletter%' THEN all_conversions
            ELSE conversions
        END as conversions,

        -- SAFETY NET: Force 0.0 Value for non-Purchases (Preserved)
        CASE
            -- Rule 1: Kill the "Registration" value immediately
            WHEN conversion_action_name IN ('Registration', 'Prospect') THEN 0.0

            -- Allow Registrations if it carries a monetary purchase value or is categorized as PURCHASE
            WHEN conversion_action_name = 'Registrations' AND (conversion_value > 0 OR conversion_category = 'PURCHASE') THEN conversion_value

            -- Rule 2: Allow valid purchase categories
            WHEN (
                conversion_action_name LIKE '%Ticket%' OR
                conversion_category = 'PURCHASE' OR
                conversion_action_name = 'NOTWORKING-Do NOT USE'
            ) THEN conversion_value
            ELSE 0.0
        END as conversion_value,

        -- Context
        currency

    FROM {{ ref('stg_google_ads_conversion_action') }}
),

meta_ads AS (
    SELECT
        id as unique_id,
        'Meta Ads' as platform,
        date,
        -- Standard Date Parts
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,

        -- EVENT CYCLE DIMENSIONS
        week as week_display,
        week_number,
        week_number_to_sort,
        weeks_left,

        -- Dimensions
        event_name,
        event_edition,

        account_id,
        account_name,
        campaign_id,
        campaign_name,
        campaign_status,
        ad_group_name,
        ad_name,

        -- Pricing / Rate Context
        cut_off_rate,
        cut_off_rate_sort_order,

        -- Conversion Details
        conversion_action as conversion_name,
        conversion_action as conversion_raw_category,

        -- STRICT META MAPPING LOGIC (Preserved)
        CASE
            -- THE CHOSEN ONE (Reliable Web Pixel Purchase)
            WHEN conversion_action = 'offsite_conversion.fb_pixel_purchase' THEN 'PURCHASE'

            -- LEADS & SIGNUPS
            WHEN conversion_action = 'offsite_conversion.fb_pixel_lead' THEN 'LEAD'
            WHEN LOWER(conversion_action) LIKE '%complete_registration%' THEN 'SIGNUP'
            WHEN LOWER(conversion_action) LIKE '%subscribe%' THEN 'SIGNUP'

            -- COMMERCE INTENT (Pre-Purchase)
            WHEN LOWER(conversion_action) LIKE '%add_to_cart%' THEN 'ADD_TO_CART'
            WHEN LOWER(conversion_action) LIKE '%checkout%' THEN 'INITIATE_CHECKOUT'
            WHEN LOWER(conversion_action) LIKE '%add_payment_info%' THEN 'INITIATE_CHECKOUT'

            -- SITE ACTIVITY
            WHEN LOWER(conversion_action) LIKE '%content_view%' THEN 'PAGE_VIEW'
            WHEN LOWER(conversion_action) LIKE '%view_content%' THEN 'PAGE_VIEW'
            WHEN LOWER(conversion_action) LIKE '%landing_page_view%' THEN 'PAGE_VIEW'
            WHEN LOWER(conversion_action) LIKE '%page_view%' THEN 'PAGE_VIEW'
            WHEN LOWER(conversion_action) LIKE '%app_site_visit%' THEN 'PAGE_VIEW'

            -- CONTACT / SEARCH
            WHEN LOWER(conversion_action) LIKE '%search%' THEN 'SEARCH'
            WHEN LOWER(conversion_action) LIKE '%contact%' THEN 'CONTACT'
            WHEN LOWER(conversion_action) LIKE '%call%' THEN 'CONTACT'
            WHEN LOWER(conversion_action) LIKE '%messaging%' THEN 'CONTACT'

            -- ENGAGEMENT (Social Actions)
            WHEN LOWER(conversion_action) LIKE '%engagement%' THEN 'ENGAGEMENT'
            WHEN LOWER(conversion_action) LIKE '%like%' THEN 'ENGAGEMENT'
            WHEN LOWER(conversion_action) LIKE '%comment%' THEN 'ENGAGEMENT'
            WHEN LOWER(conversion_action) LIKE '%reaction%' THEN 'ENGAGEMENT'
            WHEN LOWER(conversion_action) LIKE '%video_view%' THEN 'ENGAGEMENT'
            WHEN LOWER(conversion_action) LIKE '%post%' THEN 'ENGAGEMENT'

            ELSE 'OTHER'
        END as standardized_conversion_type,

        -- Metrics
        conversions,

        -- SAFETY CHECK: Zero out revenue for non-monetary events (Preserved)
        CASE
            WHEN conversion_action = 'offsite_conversion.fb_pixel_purchase' THEN conversion_value
            ELSE 0.0
        END as conversion_value,

        -- Context
        currency

    FROM {{ ref('stg_meta_ads_conversion_action') }}
)

-- COMBINE BOTH PLATFORMS
SELECT * FROM google_ads
UNION ALL
SELECT * FROM meta_ads