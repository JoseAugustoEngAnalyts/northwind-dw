with
    fonte_funcionarios as (
        select 
            cast (employee_id as int) as id_funcionario
            ,  cast (last_name as string) as ultimo_nome
            ,  cast (first_name as string) as primeiro_nome
            ,  cast (title as string) as funcao_do_funcionario
            ,  cast (title_of_courtesy as string) as titulo_cortesia
            ,  cast (birth_date as date) as data_de_aniversario
            ,  cast (hire_date as date) as data_de_contratacao
            ,  cast (address as string) as endereco_do_funcionario
            ,  cast (city as string) as cidade_do_funcionario
            ,  cast (region as string) as regiao_do_funcionario
            ,  cast (postal_code as string) as cep_do_funcionario
            ,  cast (country as string) as pais_do_funcionario
            ,  cast (home_phone as string) as telefone_do_funcionario
            ,  cast (extension as int) as extensao_funcionario
            --,photo
            ,  cast (notes as string) as descricao_curriculo
            ,  cast (reports_to as int) as hierarquia_do_funcionario
            --,photo_path 
        from {{ source('erp', 'employees') }}
    )

    select *
    from fonte_funcionarios

