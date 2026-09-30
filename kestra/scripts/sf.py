"""Conexión a Snowflake con el usuario de servicio (llave RSA, sin contraseña)."""
import os

import snowflake.connector
from cryptography.hazmat.primitives import serialization


def connect(schema=None):
    with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as fh:
        pkey = serialization.load_pem_private_key(fh.read(), password=None)
    der = pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=der,
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=schema,
        session_parameters={"QUERY_TAG": "pset2_ingest", "TIMEZONE": "America/Guayaquil"},
    )
