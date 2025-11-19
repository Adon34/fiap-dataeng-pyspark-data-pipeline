# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    ArrayType,
    DateType,
    FloatType,
    TimestampType,
    BooleanType,
)
from src.processing.transformations import Transformation
from datetime import datetime

@pytest.fixture(scope="session")
def spark_session():
    """
    Cria uma SparkSession para ser usada em todos os testes.
    A sessão é finalizada automaticamente ao final da execução dos testes.
    """
    spark = SparkSession.builder \
        .appName("PySpark Unit Tests") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_add_valor_total_pedidos(spark_session):
    """
    Testa a função add_valor_total_pedidos para garantir que a coluna 'valor_total'
    é calculada corretamente.
    """
    # 1. Arrange (Preparar os dados de entrada e o resultado esperado)
    transformer = Transformation()

    
    schema_entrada = StructType(
        [
            StructField("id_pedido", StringType(), True),
            StructField("produto", StringType(), True),
            StructField("valor_unitario", FloatType(), True),
            StructField("quantidade", LongType(), True),
            StructField("data_criacao", TimestampType(), True),
            StructField("uf", StringType(), True),
            StructField("id_cliente", LongType(), True)
        ]
    )
    
    
    dados_entrada = [
        ("i3b1g7","CELULAR", 15.8, 2,datetime(2023, 1, 15, 10, 30, 0),"SP",86),
        ("g5b581","MOUSE", 29.6, 3,datetime(2023, 1, 15, 11, 45, 15),"SP",148),
        ("n3v3g7","PILHA", 10.2, 5,datetime(2023, 1, 16, 9, 0, 0),"SP",33)
    ]
    
    
    df_entrada = spark_session.createDataFrame(dados_entrada, schema_entrada)

    schema_esperado = StructType([
        StructField("id_pedido", StringType(), True),
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
        StructField("data_criacao", TimestampType(), True),
        StructField("uf", StringType(), True),
        StructField("id_cliente", LongType(), True),
        StructField("valor_total", FloatType(), True)
    ])
    dados_esperados = [
        ("i3b1g7","CELULAR", 15.8, 2,datetime(2023, 1, 15, 10, 30, 0),"SP",86,31.6),
        ("g5b581","MOUSE", 29.6, 3,datetime(2023, 1, 15, 11, 45, 15),"SP",148,88.8),
        ("n3v3g7","PILHA", 10.2, 5,datetime(2023, 1, 16, 9, 0, 0),"SP",33,51.0)
    ]
    
    df_esperado = spark_session.createDataFrame(dados_esperados, schema_esperado)

    # 2. Act (Executar a função a ser testada)
    df_resultado = transformer.add_valor_total_pedidos(df_entrada)

    # 3. Assert (Verificar se o resultado é o esperado)
    # Coletamos os dados dos DataFrames para comparar como listas de dicionários
    resultado_coletado = sorted([row.asDict() for row in df_resultado.collect()], key=lambda x: x['produto'])
    esperado_coletado = sorted([row.asDict() for row in df_esperado.collect()], key=lambda x: x['produto'])

    assert df_resultado.count() == df_esperado.count(), "O número de linhas não corresponde ao esperado."
    assert df_resultado.columns == df_esperado.columns, "As colunas não correspondem ao esperado."
    assert resultado_coletado == esperado_coletado, "O conteúdo dos DataFrames não é igual."
