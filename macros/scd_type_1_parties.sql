{% macro scd_type1() %}
    
    merge into "ODIN_DEV"."DIM"."PARTIES_SCD_TEST" M
         using "ODIN_DEV"."LANDING_SFDC"."TEMP_PARTIES" C
            on M.PARTY_ID = C.PARTY_ID
          when matched
           and M.PARTY_NAME <> C.PARTY_NAME
           and M.DBT_VALID_TO is NULL
            or M.PARTY_TYPE <> C.PARTY_TYPE
            or M.CUSTOMER_KEY <> C.CUSTOMER_KEY
          then update
           set M.PARTY_NAME = C.PARTY_NAME,
               M.PARTY_TYPE = C.PARTY_TYPE,
               M.CUSTOMER_KEY = C.CUSTOMER_KEY

{% endmacro %}