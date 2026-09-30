-- =====================================================================
-- PSet #2 ECU 911 - Setup de Snowflake (ejecutar UNA vez)
-- Snowsight > Worksheet nuevo, rol ACCOUNTADMIN, "Run All".
-- Crea: rol, warehouse XS, base de datos y un usuario de servicio que
-- se autentica con llave RSA (sin contraseña ni MFA).
-- Para reproducir en otra cuenta: generar una llave propia (ver README)
-- y reemplazar rsa_public_key.
-- =====================================================================
use role accountadmin;

create role if not exists PSET2_ROLE;
grant role PSET2_ROLE to role SYSADMIN;

-- Para que tu usuario también pueda ver las tablas en Snowsight
set my_user = current_user();
grant role PSET2_ROLE to user identifier($my_user);

create warehouse if not exists PSET2_WH
  warehouse_size = 'XSMALL'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true;
grant usage, operate on warehouse PSET2_WH to role PSET2_ROLE;

create database if not exists PSET2_DB;
grant ownership on database PSET2_DB to role PSET2_ROLE copy current grants;

create user if not exists PSET2_SVC
  type = SERVICE
  default_role = PSET2_ROLE
  default_warehouse = PSET2_WH
  default_namespace = PSET2_DB
  rsa_public_key = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmGaZy/rzqAbRpv4pu5M2d+YW8pv9/sG3LMBIqEPdv2KsXoHrj4FiuDu+K7WKt31VdlOqDMXU7Cv7azxQ37D7TYu1ikptM4hcuguflWVpU+qNuEGoGmV07w5OTvUfo3VXHH50hqurFif+a8p9T5y9UUVxGcklVVZ+qVlZIsCI78rmsViGPnWyp1fpW1GZsbDrufE2Peikh3yg2WWCu+YBFetWSWOvzEum2fnXM6Mbc5xjqNGtVHh3yrWKcNKVwwo3Hi+Mq42fSirytlu/lXJzJD0xsh05TCkB0F9VVY7RfzNpMnGl07biGOmxNwx8Owxfw/Aw1FqavlV/UEaQKlLEnQIDAQAB';
grant role PSET2_ROLE to user PSET2_SVC;

-- Identificador de cuenta para el .env (ORGANIZACION-CUENTA)
select current_organization_name() || '-' || current_account_name() as SNOWFLAKE_ACCOUNT;
