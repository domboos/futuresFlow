CREATE TABLE dbo.cftc_cit
(
cit_id bigint NOT NULL,
market_and_exchange_names varchar NOT NULL,
report_date date NOT NULL,
cftc_contract_market_code bigint NOT NULL,
contract_market_name varchar NOT NULL,
yyyy_report_week_ww varchar NOT NULL,
cftc_market_code varchar NOT NULL,
cftc_region_code varchar NOT NULL,
cftc_commodity_code varchar NOT NULL,
commodity_name varchar NOT NULL,
open_interest_all bigint NOT NULL,
ncomm_postions_long_all_nocit bigint NOT NULL,
ncomm_postions_short_all_nocit bigint NOT NULL,
ncomm_postions_spread_all_nocit bigint NOT NULL,
comm_positions_long_all_nocit bigint NOT NULL,
comm_positions_short_all_nocit bigint NOT NULL,
tot_rept_positions_long_all bigint NOT NULL,
tot_rept_positions_short bigint NOT NULL,
nonrept_positions_long_all bigint NOT NULL,
nonrept_positions_short_all bigint NOT NULL,
cit_positions_long_all bigint NOT NULL,
cit_positions_short_all bigint NOT NULL,
traders_tot_all bigint NOT NULL,
traders_noncomm_Long_all_nocit bigint NOT NULL,
traders_noncomm_short_all_nocit bigint NOT NULL,
traders_noncomm_spread_all_nocit bigint NOT NULL,
traders_comm_long_all_nocit bigint NOT NULL,
traders_comm_short_all_nocit bigint NOT NULL,
traders_tot_rept_long_all_nocit bigint NOT NULL,
traders_tot_rept_short_all_nocit bigint NOT NULL,
traders_cit_long_all bigint NOT NULL,
traders_cit_short_all bigint NOT NULL,
contract_units varchar NOT NULL,
commodity varchar NOT NULL,
commodity_subgroup_name varchar NOT NULL,
commodity_group_name varchar NOT NULL,
CONSTRAINT cftc_cit_pkey PRIMARY KEY (cit_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;