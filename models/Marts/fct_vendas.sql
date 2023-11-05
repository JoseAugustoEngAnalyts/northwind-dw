with
    dim_produtos as (
        select * 
        from {{ ref("Dim_produtos") }}
    )
        
    ,dim_clientes as (
        select * 
        from {{ ref("Dim_clientes") }}
    )
        
    ,pedidos_itens as (
        select * 
        from {{ ref("int_vendas__pedidos_itens") }}
     )
        
    ,join_tabelas as (
        select
            pedidos_itens.sk_ordem_item
            ,dim_clientes.pk_cliente
            ,dim_produtos.pk_produto
            ,pedidos_itens.id_ordem
            ,pedidos_itens.id_funcionario
            ,pedidos_itens.id_transportadora
            ,pedidos_itens.data_do_pedido
            ,pedidos_itens.frete
            ,pedidos_itens.destinatario
            ,pedidos_itens.endereco_do_destinatario
            ,pedidos_itens.cep_do_destinatario
            ,pedidos_itens.cidade_do_destinatario
            ,pedidos_itens.regiao_do_destinatario
            ,pedidos_itens.pais_do_destinatario
            ,pedidos_itens.data_do_envio
            ,pedidos_itens.data_requerida_entrega
            ,pedidos_itens.desconto_perc_ordem_detalhe
            ,pedidos_itens.preco_da_unidade
            ,pedidos_itens.quantidade_ordem_detalhe
            ,dim_produtos.nome_produto
            ,dim_produtos.nome_categoria
            ,dim_produtos.nome_fornecedor
            ,dim_produtos.is_discontinuado
            ,dim_clientes.nome_cliente
        from pedidos_itens
        left join dim_produtos on 
        pedidos_itens.id_produto = dim_produtos.id_produto
        left join dim_clientes on
        pedidos_itens.id_cliente = dim_clientes.id_cliente
    )
    
    ,transformacoes as (
        select*
        ,(preco_da_unidade * quantidade_ordem_detalhe) as total_bruto
        ,((1 - desconto_perc_ordem_detalhe)* preco_da_unidade* quantidade_ordem_detalhe) as total_liquido
        ,case
            when desconto_perc_ordem_detalhe > 0 then true
            when desconto_perc_ordem_detalhe < 0 then false
            else false
        end as is_desconto
        ,frete / count(id_ordem) over (partition by id_ordem) as frete_ponderado
        from join_tabelas
    )

select *
from transformacoes
 

