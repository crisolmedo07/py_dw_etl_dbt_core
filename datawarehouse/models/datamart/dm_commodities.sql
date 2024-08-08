with commodities as (
    select 
        data,
        simbolo,
        valor_fechamento
    from 
        {{ref('stg_commodities')}}
) , 
movimentacao as(
    select 
        data,
        simbolo,
        acao,
        quantidade
    from 
        {{ref('stg_movimentacao_commodities')}}
),
joined as (
    select
        commodities.data,
        commodities.simbolo,
        commodities.valor_fechamento,
        movimentacao.acao,
        movimentacao.quantidade,
        (movimentacao.quantidade * commodities.valor_fechamento ) as valor,
        movimentacao.quantidade * commodities.valor_fechamento
            * case when movimentacao.acao = 'sell'  then 1  else -1 end as ganho
    from commodities 
    inner join movimentacao 
    on movimentacao.data = commodities.data
    and commodities.simbolo = movimentacao.simbolo
), 
last_day as (
    select
        max(data) as max_date
    from joined
), 
filtered as (
    select * 
    from joined
    where
        data = (select max_date from last_day)
)

select *
from filtered