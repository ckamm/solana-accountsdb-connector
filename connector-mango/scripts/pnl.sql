with
    inputs as (select
        1 as market_index,
        (select pubkey_id from pubkey
            where pubkey = '98pjRuQjK3qA6gXts96PqZT4Ze5QmnCmt3QYjhbUSPue')
            as group_pubkey_id
    ),
    group_and_cache as (select
        mango_group,
        mango_cache
        from
            inputs,
            mango_group_processed mango_group,
            mango_cache_processed mango_cache
        where
            mango_group.pubkey_id = inputs.group_pubkey_id
            and mango_cache.pubkey_id = mango_group.mango_cache_id
    ),
    params as (select
        (mango_group).perp_markets[market_index].base_lot_size
            as contract_size,
        (mango_cache).price_cache[market_index].price as price,
        (mango_cache).perp_market_cache[market_index] as pmc
        from
            group_and_cache,
            inputs
    ),
    market_perp_accounts as (select
        pubkey, (perp_accounts[market_index]).*
        from
            mango_account_processed,
            inputs
        where
            mango_group_id = group_pubkey_id
    )
select
    pubkey,
    base_position * contract_size * price +
    quote_position -
    case
        when base_position > 0 then
            ((pmc).long_funding - long_settled_funding) * base_position
        when base_position < 0 then
            ((pmc).short_funding - short_settled_funding) * base_position
        else 0
    end
    as pnl
from
    market_perp_accounts,
    params
order by pnl desc;