with
    frete_total as (
        select sum(frete_partilhado) as frete_teste
        from {{ ref('fct_vendas') }}
    )

select frete_teste
from frete_total
where frete_teste not between 64942 and 64943
