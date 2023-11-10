with
    stg_funcionarios_unica as (
    select *
    from {{ ref('stg_erp__funcionarios') }}
    )

    ,criar_chave as (
    select 
    row_number() over(order by id_funcionario) as pk_funcionario
    , *
    from stg_funcionarios_unica
    )

    select *
    from criar_chave
