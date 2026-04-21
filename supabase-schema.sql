-- ============================================================
-- Rebuild Digital Co — Tracker App
-- Supabase Schema
-- Run this in your Supabase SQL editor
-- ============================================================

-- Enable UUID extension
create extension if not exists "pgcrypto";

-- ── BUSINESSES ──────────────────────────────────────────────
create table if not exists businesses (
  id            uuid primary key default gen_random_uuid(),
  name          text not null,
  category      text,
  website       text,
  phone         text,
  address       text,
  email         text,
  opportunity_score integer default 0,
  issues        text,
  has_ssl       boolean default false,
  has_mobile    boolean default false,
  has_booking   boolean default false,
  has_portal    boolean default false,
  has_pricing   boolean default false,
  copyright_year integer,
  cms           text,
  status        text default 'new',
  -- status values: new | emailed | follow_up_due | follow_up_sent | replied | converted | not_interested
  notes         text,
  yelp_url      text,
  scraped_at    timestamptz default now(),
  created_at    timestamptz default now(),
  updated_at    timestamptz default now()
);

create index if not exists businesses_status_idx on businesses(status);
create index if not exists businesses_score_idx on businesses(opportunity_score desc);
create index if not exists businesses_category_idx on businesses(category);

-- ── EMAIL LOGS ───────────────────────────────────────────────
create table if not exists email_logs (
  id            uuid primary key default gen_random_uuid(),
  business_id   uuid references businesses(id) on delete cascade,
  subject       text,
  sent_at       timestamptz default now(),
  resend_id     text,
  from_email    text,
  to_email      text,
  status        text default 'sent',
  -- status values: sent | failed | opened | replied | bounced
  email_type    text default 'initial',
  -- email_type values: initial | follow_up_1 | follow_up_2 | custom
  error_msg     text,
  created_at    timestamptz default now()
);

create index if not exists email_logs_business_idx on email_logs(business_id);
create index if not exists email_logs_sent_at_idx on email_logs(sent_at desc);

-- ── FOLLOW UPS ───────────────────────────────────────────────
create table if not exists follow_ups (
  id            uuid primary key default gen_random_uuid(),
  business_id   uuid references businesses(id) on delete cascade,
  scheduled_for timestamptz not null,
  sent_at       timestamptz,
  status        text default 'pending',
  -- status values: pending | sent | cancelled | skipped
  follow_up_number integer default 1,
  notes         text,
  created_at    timestamptz default now()
);

create index if not exists follow_ups_scheduled_idx on follow_ups(scheduled_for);
create index if not exists follow_ups_status_idx on follow_ups(status);

-- ── FEEDBACK / REPLIES ───────────────────────────────────────
create table if not exists feedback (
  id            uuid primary key default gen_random_uuid(),
  business_id   uuid references businesses(id) on delete cascade,
  received_at   timestamptz default now(),
  channel       text default 'email',
  -- channel: email | phone | in_person | other
  message       text,
  sentiment     text default 'neutral',
  -- sentiment: positive | neutral | negative
  action_taken  text,
  created_at    timestamptz default now()
);

create index if not exists feedback_business_idx on feedback(business_id);
create index if not exists feedback_received_idx on feedback(received_at desc);

-- ── SCRAPE RUNS ──────────────────────────────────────────────
create table if not exists scrape_runs (
  id                 uuid primary key default gen_random_uuid(),
  started_at         timestamptz default now(),
  completed_at       timestamptz,
  status             text default 'running',
  -- status: running | completed | failed | cancelled
  location           text default 'Dallas, TX',
  categories         text,
  businesses_found   integer default 0,
  emails_queued      integer default 0,
  emails_sent        integer default 0,
  high_opp_count     integer default 0,
  error_message      text,
  triggered_by       text default 'manual'
);

-- ── AUTO-UPDATE updated_at ───────────────────────────────────
create or replace function update_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

create trigger businesses_updated_at
  before update on businesses
  for each row execute function update_updated_at();

-- ── ROW LEVEL SECURITY ───────────────────────────────────────
-- Enable RLS (allow all for anon key since this is a single-user app)
alter table businesses    enable row level security;
alter table email_logs    enable row level security;
alter table follow_ups    enable row level security;
alter table feedback      enable row level security;
alter table scrape_runs   enable row level security;

-- Permissive policies (single user, private app)
create policy "Allow all" on businesses    for all using (true) with check (true);
create policy "Allow all" on email_logs    for all using (true) with check (true);
create policy "Allow all" on follow_ups    for all using (true) with check (true);
create policy "Allow all" on feedback      for all using (true) with check (true);
create policy "Allow all" on scrape_runs   for all using (true) with check (true);

-- ── USEFUL VIEWS ─────────────────────────────────────────────
create or replace view business_summary as
select
  b.id,
  b.name,
  b.category,
  b.website,
  b.email,
  b.phone,
  b.opportunity_score,
  b.status,
  b.scraped_at,
  count(distinct el.id)  as emails_sent,
  count(distinct fu.id)  as follow_ups_scheduled,
  count(distinct fb.id)  as feedback_count,
  max(el.sent_at)        as last_emailed_at,
  min(fu.scheduled_for) filter (where fu.status = 'pending') as next_follow_up
from businesses b
left join email_logs el on el.business_id = b.id
left join follow_ups fu on fu.business_id = b.id
left join feedback fb   on fb.business_id = b.id
group by b.id;
