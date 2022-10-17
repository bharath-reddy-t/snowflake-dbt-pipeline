{% macro scd_type1(party_name, party_type, customer_key, source_table) %}
    
    merge into "ODIN_DEV"."DIM"."PARTIES_SCD_TEST" M
         using "ODIN_DEV"."LANDING_SFDC"."TEMP_PARTIES" C
            on M.PARTY_ID = C.PARTY_ID
          when matched
           and M.{{party_name}} <> C.PARTY_NAME
           and M.DBT_VALID_TO is NULL
            or M.{{party_type}} <> C.PARTY_TYPE
            or M.{{customer_key}} <> C.CUSTOMER_KEY
          then update
           set M.{{party_name}} = C.PARTY_NAME,
               M.{{party_type}} = C.PARTY_TYPE,
               M.{{customer_key}} = C.CUSTOMER_KEY

{% endmacro %}