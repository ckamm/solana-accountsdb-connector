use mango::state::{DataType, MangoAccount};
use mango_common::Loadable;
use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;
use postgres_types::ToSql;

use log::*;

use std::{mem::size_of};

use crate::{AccountTable, AccountWrite};

pub struct MangoAccountTable {}

#[derive(Debug, ToSql)]
struct PerpAccount {
    base_position: i64,
    quote_position: f64,
    long_settled_funding: f64,
    short_settled_funding: f64,
    bids_quantity: i64,
    asks_quantity: i64,
    taker_base: i64,
    taker_quote: i64,
    mngo_accrued: i64,
}

#[async_trait]
impl AccountTable for MangoAccountTable {
    fn table_name(&self) -> &str {
        "mango_account_write"
    }

    async fn insert_account_write(
        &self,
        client: &postgres_query::Caching<tokio_postgres::Client>,
        account_write: &AccountWrite,
    ) -> Result<(), anyhow::Error> {
        if account_write.data.len() != size_of::<MangoAccount>() || account_write.data[0] != DataType::MangoAccount as u8 {
            return Ok(());
        }

        let pubkey: &[u8] = &account_write.pubkey.to_bytes();
        let data = MangoAccount::load_from_bytes(&account_write.data)?;

        // TODO fix I80F48, fix i128, fix u64, fix PerpAccount
        let owner: &[u8] = &data.owner.to_bytes();
        let mango_group: &[u8] = &data.mango_group.to_bytes();
        let version = data.meta_data.version as i16;
        let extra_info = &data.meta_data.extra_info as &[u8];
        let in_margin_basket = &data.in_margin_basket as &[bool];
        let num_in_margin_basket = data.num_in_margin_basket as i16;
        let deposits = data.deposits.iter().map(|v| v.to_num::<f64>()).collect::<Vec<f64>>();
        let borrows = data.borrows.iter().map(|v| v.to_num::<f64>()).collect::<Vec<f64>>();
        let spot_open_orders = data.spot_open_orders.iter().map(|key| key.to_bytes().to_vec()).collect::<Vec<Vec<u8>>>();
        let perp_accounts = data.perp_accounts.iter().map(|perp| PerpAccount {
            base_position: perp.base_position,
            quote_position: perp.quote_position.to_num::<f64>(),
            long_settled_funding: perp.long_settled_funding.to_num::<f64>(),
            short_settled_funding: perp.short_settled_funding.to_num::<f64>(),
            bids_quantity: perp.bids_quantity,
            asks_quantity: perp.asks_quantity,
            taker_base: perp.taker_base,
            taker_quote: perp.taker_quote,
            mngo_accrued: perp.mngo_accrued as i64,
        }).collect::<Vec<PerpAccount>>();
        let order_market = data.order_market.iter().map(|v| *v as i16).collect::<Vec<i16>>();
        let order_side = data.order_side.iter().map(|v| *v as i16).collect::<Vec<i16>>();
        let orders = data.orders.iter().map(|v| *v as f64).collect::<Vec<f64>>();
        let client_order_ids = data.client_order_ids.iter().map(|v| *v as i64).collect::<Vec<i64>>();
        let msrm_amount = data.msrm_amount as i64;
        let info = &data.info as &[u8];
        let advanced_orders_key = &data.advanced_orders_key.to_bytes() as &[u8];
        let padding = &data.padding as &[u8];

        let query = postgres_query::query!(
            "
            INSERT INTO mango_account_write
            (pubkey, slot, write_version,
            version, is_initialized, extra_info, mango_group,
            owner, in_margin_basket, num_in_margin_basket, deposits,
            borrows, spot_open_orders, perp_accounts, order_market,
            order_side, orders, client_order_ids,
            msrm_amount, being_liquidated, is_bankrupt, info,
            advanced_orders_key, padding
            )
            VALUES
            ($pubkey, $slot, $write_version,
            $version, $is_initialized, $extra_info, $mango_group,
            $owner, $in_margin_basket, $num_in_margin_basket, $deposits,
            $borrows, $spot_open_orders, $perp_accounts, $order_market,
            $order_side, $orders, $client_order_ids,
            $msrm_amount, $being_liquidated, $is_bankrupt, $info,
            $advanced_orders_key, $padding
            )
            ON CONFLICT (pubkey, slot, write_version) DO NOTHING",
            pubkey,
            slot = account_write.slot,
            write_version = account_write.write_version,
            version,
            is_initialized = data.meta_data.is_initialized,
            extra_info,
            mango_group,
            owner,
            in_margin_basket,
            num_in_margin_basket,
            deposits,
            borrows,
            spot_open_orders,
            perp_accounts,
            order_market,
            order_side,
            orders,
            client_order_ids,
            msrm_amount,
            being_liquidated = data.being_liquidated,
            is_bankrupt = data.is_bankrupt,
            info,
            advanced_orders_key,
            padding,
        );
        let _ = query.execute(client).await?;
        Ok(())
    }
}
