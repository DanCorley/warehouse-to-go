Welcome to the snowflake demo dbt project!

## Using the starter project

Create a snowflake trial account and set up some of the objects like:

```sql
set USER = '{YOUR_USERNAME}';

create database dbt_database;
alter warehouse compute_wh set
  auto_resume = true
  auto_suspend = 60
;
create role developer_role;
grant ownership on database dbt_database
  to role developer_role
;
grant role developer_role to user identifier($USER)
;
grant usage on warehouse compute_wh
  to role developer_role
;
grant imported privileges on database snowflake_sample_data
  to role developer_role
;
alter user identifier($USER) set
  default_namespace = analytics
  default_warehouse = compute_wh
  default_role = developer_role
;
```

I also highly suggest setting up [key:pair for authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth#generate-the-private-key)

### Set up your dbt profile:

🥳 notice two targets to make it easy to switch from snowflake <-> duckdb 🥳

```yml
portable_warehouse:
  outputs:
    snow:
      # select concat_ws('-', current_organization_name(), current_account_name());
      account: {ORG_NAME-ACCT_NAME} 
      database: dbt_database
      # please use key:pair 😢
      password: $0upErs3cRet!
      # if using key:pair
      private_key_path: /path/to/private.p8
      role: developer_role
      schema: dev
      threads: 4
      type: snowflake
      user: {USER_NAME}
      warehouse: compute_wh
    duck:
      type: duckdb
      path: warehouse_mirror.duckdb
  target: snow
```

Steps to setup your own portable warehouse:
  - Test that your connection works: `dbt debug`
  - Run dbt on snowflake: `dbt run -t snow`
  - Verify warehouse-to-go connects: `warehouse-to-go debug`
  - Show schemas available: `warehouse-to-go analyze`
  - Pull down all data: `warehouse-to-go extract`
  - Run dbt on duckdb: `dbt run -t duck`


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
