Hi Adam,

Unfortunately, it looks like we can't use this data since it is summarized and not the raw event data. It seems Fathom and Plausible do the same thing where they lock you into their platform.

With Umami, you can always export your raw data in case you ever want to self-host. I hope this isn't a deal breaker for you.

Regards,
Mike @ Umami


Hey Mike,

Thanks for taking a look! That’s too bad it won’t cleanly import. The main thing I’d want to keep are visits / date and countries / date. I wonder if I used the fathom logs to generate synthetic data that matched the counts/countries, that’d be enough for my purposes to migrate. Could you send me over a template of what would cleanly import, and I can try to generate data that’d match the numbers fathom gives me. If I’m able to get that working, that’d be great.

Thanks,

Adam



Hi Adam,

Sure, here is our schema for website events:

    website_id UUID,
    session_id UUID,
    visit_id UUID,
    event_id UUID,
    --sessions
    hostname String,
    browser  String,
    os  String,
    device  String,
    screen  String,
    language  String,
    country  String,
    region  String,
    city String,
    --pageviews
    url_path String,
    url_query String,
    utm_source String,
    utm_medium String,
    utm_campaign String,
    utm_content String,
    utm_term String,
    referrer_path String,
    referrer_query String,
    referrer_domain String,
    page_title String,
    --clickIDs
    gclid String,
    fbclid String,
    msclkid String,
    ttclid String,
    li_fat_id String,
    twclid String,
    --events
    event_type UInt32,
    event_name String,
    tag String,
    distinct_id String,
    created_at DateTime('UTC')

Hope that helps.Let me know if you have any questions.

Regards,
Mike
