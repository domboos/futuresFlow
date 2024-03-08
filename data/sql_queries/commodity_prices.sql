with selection as (
select px_id,bb_tkr  from cftc.fut_desc fd
where roll = 'active_futures' and adjustment ='by_ratio'
)
select selection.bb_tkr, d.px_date ,d.qty from cftc."data" d
left join selection on selection.px_id = d.px_id
where bb_tkr <> 'XB'
