# Turkey Energy Market — dbt Transformation Layer

This repository contains the dbt data transformation models for the Turkey Energy Market Analytics Pipeline.

## Overview

Transforms raw electricity market data (EPIAS) and inflation data (TCMB) from Snowflake RAW schema into analytics-ready mart tables using dbt Core.

**Main repository:** [analytics-pipeline](https://github.com/aycaize/analytics-pipeline)

## Models

| Layer | Models |
|-------|--------|
| Staging | stg_prices, stg_generation, stg_consumption, stg_cpi |
| Intermediate | int_hourly_market |
| Fact | fact_hourly_prices, fact_hourly_generation, fact_hourly_consumption |
| Mart | mart_price_volatility, mart_renewable_share, mart_price_seasonal, mart_cpi |

## Testing

46 data quality tests including not_null, unique, relationships and accepted_values.

## Tech Stack

dbt Core 1.7 · Snowflake · GitHub Actions CI
