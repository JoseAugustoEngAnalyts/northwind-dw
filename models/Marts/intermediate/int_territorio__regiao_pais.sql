with 
 stg_territorio as (
    select *
    from {{ ref('stg_erp__territorio') }}
 )

 ,stg_regiao as (
    select *
    from {{ ref('stg_erp__regiao') }}
 )

 ,join_tabelas as (
    select
    stg_territorio.id_territorio
    , stg_territorio.id_regiao
    , stg_territorio.descricao_territorio
    , stg_regiao.descricao_regiao  
    from stg_territorio
    left join stg_regiao on
    stg_territorio.id_regiao = stg_regiao.id_regiao
 )

 ,criar_chave as (
    select 
    id_territorio as pk_territorio
    ,*
    from join_tabelas
 )
 
 select *
 from criar_chave
