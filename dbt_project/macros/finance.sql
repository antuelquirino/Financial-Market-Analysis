{#- Financial conventions shared by every model. Values come from vars in dbt_project.yml. -#}

{% macro risk_free_daily() -%}
    ({{ var('risk_free_rate_annual') }} / {{ var('trading_days_per_year') }})
{%- endmacro %}

{% macro annualize_volatility(daily_stddev) -%}
    ({{ daily_stddev }} * sqrt({{ var('trading_days_per_year') }}))
{%- endmacro %}

{#- Sharpe ratio from daily simple returns: mean excess return over its
    standard deviation, annualized by sqrt(trading days). -#}
{% macro annualized_sharpe(mean_daily_return, daily_stddev) -%}
    (safe_divide({{ mean_daily_return }} - {{ risk_free_daily() }}, nullif({{ daily_stddev }}, 0))
        * sqrt({{ var('trading_days_per_year') }}))
{%- endmacro %}
