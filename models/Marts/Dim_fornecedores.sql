with    
    stg_fornecedores as (
        select *
        from {{ ref('stg_erp__fornecedores') }}
    )

    ,stg_produtos as (
        select *
        from {{ ref('stg_erp__produtos') }}
    )

    , join_tabelas as (
        select 
         stg_produtos.id_produto
        , stg_fornecedores.id_fornecedor
        , stg_fornecedores.nome_fornecedor
        , stg_fornecedores.contato_fornecedor
        , stg_fornecedores.contato_funcao
        , stg_fornecedores.endereco_fornecedor
        , stg_fornecedores.cidade_fornecedor
        , stg_fornecedores.regiao_fornecedor
        , stg_fornecedores.cep_fornecedor
        , stg_fornecedores.pais_fornecedor
        from stg_produtos
        left join stg_fornecedores on
        stg_produtos.id_fornecedor = stg_fornecedores.id_fornecedor
    )

    , criar_chave as (
        select 
        row_number() over(order By id_produto) as pk_produto
        , *
        from join_tabelas
    )

select *
from criar_chave


