# src/processing/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class Transformation:
    """
    Classe que contém as transformações e regras de negócio da aplicação.
    """

    def get_filter(self, pagamento_df: DataFrame) -> DataFrame:
        """Filtra dados cujo o pagamento foi recusado mesmo sendo legitimos."""
        return pagamento_df.filter(
            (pagamento_df["status"] == "false")
            & (pagamento_df["avaliacao_fraude.fraude"] == "false")
        )

    def add_valor_total_pedidos(self, pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna 'valor_total' (valor_unitario * quantidade) ao DataFrame de pedidos."""
        return pedidos_df.withColumn(
            "valor_total", F.col("valor_unitario") * F.col("quantidade")
        )

    def get_select(self, pedidos_pag: DataFrame) -> DataFrame:
        """Seleciona colunas: Identificador do pedido, Estado (UF), Forma de pagamento, Valor total do pedido e Data do pedido."""
        return pedidos_pag.select(
            pedidos_pag.id_pedido,
            pedidos_pag.uf,
            pedidos_pag.forma_pagamento,
            pedidos_pag.valor_total,
            pedidos_pag.data_criacao,
        )

    def join_pagamentos_pedidos(
        self, pagamento_df: DataFrame, pedidos_df: DataFrame
    ) -> DataFrame:
        """Faz a junção entre os DataFrames de pagamentos e pedidos."""
        return pagamento_df.join(pedidos_df, on="id_pedido", how="inner")

    def get_filter_order(self, pedidos_pag: DataFrame) -> DataFrame:
        """Filtra dados de 2025 e ordena por uf, forma de pagamento e data de criação do pedido."""
        return pedidos_pag.filter(
            pedidos_pag["data_criacao"] >= "2025-01-01 00:00:00"
        ).orderBy(
            ["uf", "forma_pagamento", "data_criacao"], ascending=[True, True, True]
        )
