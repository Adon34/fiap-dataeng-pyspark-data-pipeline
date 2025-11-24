# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    TimestampType, DecimalType
)
from decimal import Decimal
from datetime import datetime
from src.processing.transformations import Transformation

# Fixtures no Pytest geralmente ficam soltas ou no conftest.py, 
# mas funcionam perfeitamente sendo injetadas nos métodos da classe.
@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.appName("PySpark Unit Tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()

class TestTransformations:
    """
    Suite de testes encapsulada para as transformações de dados.
    """

    def test_add_valor_total_pedidos(self, spark_session):
        """
        Testa o cálculo de valor total.
        Nota: 'self' é obrigatório pois agora é um método de classe.
        """
        # 1. Arrange
        transformer = Transformation()

        schema_entrada = StructType([
            StructField("id_pedido", StringType(), True),
            StructField("produto", StringType(), True),
            StructField("valor_unitario", DecimalType(10, 2), True),
            StructField("quantidade", LongType(), True),
            StructField("data_criacao", TimestampType(), True),
            StructField("uf", StringType(), True),
            StructField("id_cliente", LongType(), True),
        ])

        dados_entrada = [
            ("i3b1g7", "CELULAR", Decimal("15.80"), 2, datetime(2023, 1, 15, 10, 30, 0), "SP", 86),
            ("g5b581", "MOUSE", Decimal("29.60"), 3, datetime(2023, 1, 15, 11, 45, 15), "SP", 148),
            ("n3v3g7", "PILHA", Decimal("10.20"), 5, datetime(2023, 1, 16, 9, 0, 0), "SP", 33),
        ]

        df_entrada = spark_session.createDataFrame(dados_entrada, schema_entrada)

        schema_esperado = StructType([
            StructField("id_pedido", StringType(), True),
            StructField("produto", StringType(), True),
            StructField("valor_unitario", DecimalType(10, 2), True),
            StructField("quantidade", LongType(), True),
            StructField("data_criacao", TimestampType(), True),
            StructField("uf", StringType(), True),
            StructField("id_cliente", LongType(), True),
            StructField("valor_total", DecimalType(10, 2), True),
        ])

        dados_esperados = [
            ("i3b1g7", "CELULAR", Decimal("15.80"), 2, datetime(2023, 1, 15, 10, 30, 0), "SP", 86, Decimal("31.60")),
            ("g5b581", "MOUSE", Decimal("29.60"), 3, datetime(2023, 1, 15, 11, 45, 15), "SP", 148, Decimal("88.80")),
            ("n3v3g7", "PILHA", Decimal("10.20"), 5, datetime(2023, 1, 16, 9, 0, 0), "SP", 33, Decimal("51.00")),
        ]

        df_esperado = spark_session.createDataFrame(dados_esperados, schema_esperado)

        # 2. Act
        df_resultado = transformer.add_valor_total_pedidos(df_entrada)

        # 3. Assert
        resultado_coletado = sorted([row.asDict() for row in df_resultado.collect()], key=lambda x: x["produto"])
        esperado_coletado = sorted([row.asDict() for row in df_esperado.collect()], key=lambda x: x["produto"])

        assert df_resultado.count() == df_esperado.count()
        assert df_resultado.columns == df_esperado.columns
        assert resultado_coletado == esperado_coletado