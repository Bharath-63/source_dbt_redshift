create table IF NOT EXISTS "dev"."public"."covid_data__dbt_tmp"
(
bu VARCHAR(100),
sub_bu VARCHAR(100),
state VARCHAR(100),
acc_type VARCHAR(100),
acc_type_child VARCHAR(100),
fiscal_year VARCHAR(100),
month_code VARCHAR(100),
mtd_actual double precision,
mtd_budget double precision,
mtd_prior_year_actual double precision,
mtd_actual_vs_budget_pct double precision,
mtd_actual_vs_prior_year_pct double precision,
ytd_actual double precision,
ytd_budget double precision,
ytd_prior_year_actual double precision,
ytd_actual_vs_budget_pct double precision,
ytd_actual_vs_prior_year_pct double precision,
fy_month varchar(100),
total_budget bigint,
primekey varchar(100),
uniqu varchar(100),
sno bigint,
sno2 bigint

);
