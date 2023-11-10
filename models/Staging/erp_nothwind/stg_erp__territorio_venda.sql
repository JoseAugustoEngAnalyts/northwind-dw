with
    fonte_territorio_de_venda as (
        select 
        cast (employee_id as int) as id_funcionario
         , cast (territory_id as int) as id_territorio
        from {{ source('erp', 'employee_territories') }}
    )

    select *
    from fonte_territorio_de_venda
    
