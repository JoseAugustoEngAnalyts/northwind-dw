with
    fonte_ordensdetalhes as (
        select 
            cast (order_id as int) as id_ordem
            , cast (product_id as int) as id_produto
            , cast (discount as numeric) as desconto_perc_ordem_detalhe
            , cast (unit_price as numeric) as preco_da_unidade
            , cast (quantity as int) as quantidade_ordem_detalhe
        from {{ source('erp', 'order_details') }}
    )

    select *
    from fonte_ordensdetalhes
    