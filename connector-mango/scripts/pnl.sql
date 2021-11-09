with
    data0 as (select
        1 as market_index,
        (select pubkey_id from pubkey where pubkey = '98pjRuQjK3qA6gXts96PqZT4Ze5QmnCmt3QYjhbUSPue') as group_pubkey_id
    ),
    data1 as (select
        *,
        (select
            g from mango_group_write g
            where g.pubkey_id = group_pubkey_id
            order by g.slot desc limit 1) as mg
        from data0
    ),
    data2 as (select
        *,
        (select
            c from mango_cache_write c
            where (data1.mg).mango_cache_id = c.pubkey_id
            order by c.slot desc limit 1) as mc
        from data1
    ),
    data3 as (select
        *,
        (data2.mg).perp_markets[market_index].base_lot_size as contract_size,
        (data2.mc).price_cache[market_index].price as price,
        (data2.mc).perp_market_cache[market_index] as pmc
        from data2
    ),
    perp_accounts as (select distinct on(pubkey_id)
        pubkey, (perp_accounts[market_index]).*
        from mango_account_processed, data0
        where mango_group_id = group_pubkey_id
    )
select
    pubkey,
    base_position * contract_size * price +
    quote_position - CASE
        WHEN base_position > 0 THEN
            ((data3.pmc).long_funding - long_settled_funding) * base_position
        WHEN base_position < 0 THEN
            ((data3.pmc).short_funding - short_settled_funding) * base_position
        ELSE 0
    END
    as pnl
from
    perp_accounts
cross join data3
order by pnl desc;
