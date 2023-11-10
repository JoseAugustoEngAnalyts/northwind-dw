with
    stg_area_estruturas as (
        select*
        from {{ ref('int_funcionario__area_vendas') }}
    )

    ,stg_regiao_vendas as (
        select *
        from {{ ref('int_territorio__regiao_pais') }}
    )

    ,join_tabelas as (
        select
            stg_area_estruturas.sk_Funcionario_area_de_venda
            , stg_regiao_vendas.pk_territorio
            , stg_area_estruturas.id_funcionario
            , stg_regiao_vendas.id_territorio
            , stg_regiao_vendas.id_regiao
            , stg_regiao_vendas.descricao_territorio
            , stg_regiao_vendas.descricao_regiao
            , stg_area_estruturas.ultimo_nome
            , stg_area_estruturas.primeiro_nome
            , stg_area_estruturas.funcao_do_funcionario
            , stg_area_estruturas.titulo_cortesia
            , stg_area_estruturas.data_de_aniversario
            , stg_area_estruturas.data_de_contratacao
            , stg_area_estruturas.endereco_do_funcionario
            , stg_area_estruturas.cidade_do_funcionario
            , stg_area_estruturas.regiao_do_funcionario
            , stg_area_estruturas.cep_do_funcionario
            , stg_area_estruturas.pais_do_funcionario
            , stg_area_estruturas.telefone_do_funcionario
            , stg_area_estruturas.extensao_funcionario
            , stg_area_estruturas.descricao_curriculo
            , stg_area_estruturas.hierarquia_do_funcionario
        from stg_area_estruturas
        left join stg_regiao_vendas on
         stg_area_estruturas.id_territorio = stg_regiao_vendas.id_territorio
    )

    select *
    from join_tabelas
