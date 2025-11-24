# src/io_utils/data_handler.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    FloatType,
    TimestampType,
    BooleanType,
)
from pyspark.errors import AnalysisException
import logging

# Configuração centralizada do logging
logger = logging.getLogger(__name__)


class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pagamentos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pagamentos."""

        avaliacao_fraude_schema = StructType(
            [
                StructField("fraude", BooleanType(), True),
                StructField("score", FloatType(), True),
            ]
        )

        schema_pagamentos = StructType(
            [
                StructField(
                    "id_pedido", StringType(), False
                ),  # UUID (String), Não-nulo (False)
                StructField("forma_pagamento", StringType(), True),  # float
                StructField("valor_pagamento", FloatType(), True),  # float
                StructField("status", BooleanType(), True),  # boolean
                StructField(
                    "data_processamento", TimestampType(), True
                ),  # date/timestamp (Ex: 2024-01-01T02:55:59)
                # Objeto Aninhado, usando o schema definido acima
                StructField("avaliacao_fraude", avaliacao_fraude_schema, True),
            ]
        )

        return schema_pagamentos

    def _get_schema_pedidos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pedidos."""

        schema_pedidos = StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("produto", StringType(), True),
                StructField("valor_unitario", FloatType(), True),
                StructField("quantidade", LongType(), True),
                StructField("data_criacao", TimestampType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True),
            ]
        )

        return schema_pedidos

    def load_pagamentos(self, path: str) -> DataFrame:
        """Carrega o dataframe de clientes a partir de um arquivo JSON."""
        schema = self._get_schema_pagamentos()
        try:
            return self.spark.read.option("compression", "gzip").json(
                path, schema=schema
            )
        except AnalysisException as e:
            if "PATH_NOT_FOUND" in str(e):
                logger.error(f"Arquivo não encontrado: {path}")

            raise Exception(f"Erro ao carregar pedidos: {e}")

    def load_pedidos(
        self, path: str, compression: str, header: bool, sep: str
    ) -> DataFrame:
        """Carrega o dataframe de pedidos a partir de um arquivo CSV."""
        schema = self._get_schema_pedidos()
        try:
            return self.spark.read.option("compression", compression).csv(
                path, header=header, schema=schema, sep=sep
            )
        except AnalysisException as e:
            if "PATH_NOT_FOUND" in str(e):
                logger.error(f"Arquivo não encontrado: {path}")

            raise Exception(f"Erro ao carregar pedidos: {e}")

    def write_parquet(self, df: DataFrame, path: str):
        """
        Salva o DataFrame em formato Parquet, sobrescrevendo se já existir.

        :param df: DataFrame a ser salvo.
        :param path: Caminho de destino.
        """
        try:
            df.write.mode("overwrite").parquet(path)
            logger.info(f"Dados salvos com sucesso em: {path}")
        except AnalysisException as e:
            logger.error(f"Não foi possivel escrever arquivo: {path}. Detalhes: {e}", exc_info=True)
            raise 
