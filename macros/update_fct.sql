{% macro update_fct(coll_name) %}

    merge into "ODIN_DEV"."FACT"."FACT_OPPORTUNITY" fct
    using ( select b.OPPORTUNITY_ID, b.ACCOUNT_ID, A.ACCOUNT_ID, A.ACCOUNT_KEY, A.DBT_VALID_TO
              from "ODIN_DEV"."LANDING_SFDC"."BRIDGE_OPP_ACC" b
             inner
              join "ODIN_DEV"."DIM"."DIM_ACCOUNTS" A
                on b.ACCOUNT_ID = A.ACCOUNT_ID and A.DBT_VALID_TO is null
          ) ba
       on fct.OPPORTUNITY_ID = ba.OPPORTUNITY_ID and fct.ACCOUNT_KEY = -99
     when matched 
     then update 
      set fct.ACCOUNT_KEY = ba.ACCOUNT_KEY;

{% endmacro %}