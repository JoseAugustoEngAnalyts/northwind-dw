with
    stg_ordens as (
        select * 
        from {{ ref("stg_erp__ordens") }}
        )

    ,stg_ordem_detalhes as (
        select * 
        from {{ ref("stg_erp__ordem_detalhes") }})
        
    ,join_tabelas as (
        select
            stg_ordens.id_ordem,
            stg_ordens.id_funcionario,
            stg_ordens.id_cliente,
            stg_ordens.id_transportadora,
            stg_ordem_detalhes.id_produto,
            stg_ordens.data_do_pedido,
            stg_ordens.frete,
            stg_ordens.destinatario,
            stg_ordens.endereco_do_destinatario,
            stg_ordens.cep_do_destinatario,
            stg_ordens.cidade_do_destinatario,
            stg_ordens.regiao_do_destinatario,
            stg_ordens.pais_do_destinatario,
            stg_ordens.data_do_envio,
            stg_ordens.data_requerida_entrega,
            stg_ordem_detalhes.desconto_perc_ordem_detalhe,
            stg_ordem_detalhes.preco_da_unidade,
            stg_ordem_detalhes.quantidade_ordem_detalhe
        from stg_ordem_detalhes
        left join stg_ordens on 
        stg_ordem_detalhes.id_ordem = stg_ordens.id_ordem
    )
    
    ,criar_chave as (
        select
            cast(id_ordem as string) || cast(id_produto as string) as sk_ordem_item, *
        from join_tabelas
    )

select *
from criar_chave
