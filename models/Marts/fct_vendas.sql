with 
    Dim_produtos as (
        select *
        from {{ ref('Dim_produtos') }}
)

, Dim_clientes as (
    select *
    from {{ ref('Dim_clientes') }}
)

, Dim_transportadoras as (
    select *
    from {{ ref('Dim_transportadoras') }}
)

, Dim_funcionarios as (
    select *
    from {{ ref('Dim_funcionarios') }}
)

, pedidos_itens as (
    select *
    from {{ ref('int_vendas__pedidos_itens') }}
)

,join_tabelas as (
    select
     pedidos_itens.sk_ordem_item
        , pedidos_itens.id_ordem
        , Dim_funcionarios.pk_funcionario
        , Dim_clientes.pk_cliente
        , Dim_transportadoras.pk_transportadora
        , Dim_produtos.pk_produto
        , pedidos_itens.data_do_pedido
        , pedidos_itens.frete
        , pedidos_itens.destinatario
        , pedidos_itens.endereco_do_destinatario
        , pedidos_itens.cep_do_destinatario
        , pedidos_itens.cidade_do_destinatario
        , pedidos_itens.regiao_do_destinatario
        , pedidos_itens.pais_do_destinatario
        , pedidos_itens.data_do_envio
        , pedidos_itens.data_requerida_entrega
        , pedidos_itens.desconto_perc_ordem_detalhe
        , pedidos_itens.preco_da_unidade
        , pedidos_itens.quantidade_ordem_detalhe
    from pedidos_itens
    left join Dim_produtos on
        pedidos_itens.id_produto = Dim_produtos.id_produto
    left join Dim_clientes on
        pedidos_itens.id_cliente = Dim_clientes.id_cliente
    left join Dim_transportadoras on
        pedidos_itens.id_transportadora = Dim_transportadoras.id_transportadora
    left join Dim_funcionarios on
    pedidos_itens.id_funcionario = Dim_funcionarios.id_funcionario
)

, transformacoes as (
    select *
    ,preco_da_unidade*quantidade_ordem_detalhe as total_bruto
    ,(1-desconto_perc_ordem_detalhe)*preco_da_unidade*quantidade_ordem_detalhe as total_liquido_venda
    ,case
        when desconto_perc_ordem_detalhe > 0 then true
        when desconto_perc_ordem_detalhe < 0 then false
        else false
    end as is_desconto
    , frete / count (id_ordem) over(partition by id_ordem) as frete_partilhado
    from join_tabelas
)

select *
from transformacoes


   




