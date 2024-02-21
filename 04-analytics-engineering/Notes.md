-- to run up/down stream dbt model with variable through CLI 
dbt build --select +fact_trips+ --vars '{'is_test_run': 'false'}'


-- how to generate yaml models for your dbt models using codegen package
{% set models_to_generate = codegen.get_models(directory='staging', prefix='stg_') %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}


-- to run entire project with input variable from CLI

- dbt run --vars 'is_test_run':'false'




