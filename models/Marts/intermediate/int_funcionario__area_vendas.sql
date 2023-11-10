with    
    stg_funcionarios as (
        select *
        from {{ ref('stg_erp__funcionarios') }}
    )

    ,stg_area_de_vendas as (
    select *
    from {{ ref('stg_erp__territorio_venda') }}
    )

    ,join_tabelas as (
    select 
        stg_area_de_vendas.id_funcionario
        , stg_area_de_vendas.id_territorio
        , stg_funcionarios.ultimo_nome
        , stg_funcionarios.primeiro_nome
        , stg_funcionarios.funcao_do_funcionario
        , stg_funcionarios.titulo_cortesia
        , stg_funcionarios.data_de_aniversario
        , stg_funcionarios.data_de_contratacao
        , stg_funcionarios.endereco_do_funcionario
        , stg_funcionarios.cidade_do_funcionario
        , stg_funcionarios.regiao_do_funcionario
        , stg_funcionarios.cep_do_funcionario
        , stg_funcionarios.pais_do_funcionario
        , stg_funcionarios.telefone_do_funcionario
        , stg_funcionarios.extensao_funcionario
        , stg_funcionarios.descricao_curriculo
        , stg_funcionarios.hierarquia_do_funcionario
    from stg_area_de_vendas
    left join stg_funcionarios on
    stg_area_de_vendas.id_funcionario = stg_funcionarios.id_funcionario
    )

    ,criar_chave as (
        select 
        cast (id_funcionario as string) || cast (id_territorio as string) as sk_Funcionario_area_de_venda
        , *
        from join_tabelas
    )

select * 
from criar_chave
