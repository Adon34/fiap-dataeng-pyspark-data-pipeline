# src/pipeline/pipeline.py
from pyspark.sql import SparkSession
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
import config.settings as settings
import logging

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_handler = DataHandler(self.spark)
        self.transformer = Transformation()

    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """
        print("Pipeline iniciado...")

        print("Abrindo o dataframe de pagamentos")
        path_pagamentos = config["paths"]["pagamentos"]

        try:
            pagamentos = self.data_handler.load_pagamentos(path=path_pagamentos)
        except Exception as e:
            logger.error(f"Problemas ao carregar dados de pagamentos: {e}")
            return

        pagamentos.show(5, truncate=False)

        print("Filtrando dataframe cujo o pagamento foi recusado mesmo sendo legitimos")
        pagamentos_selecionado = self.transformer.get_filter(pagamentos)
        pagamentos_selecionado.show(5, truncate=False)

        print("Abrindo o dataframe de pedidos")
        path_pedidos = config["paths"]["pedidos"]
        compression_pedidos = config["file_options"]["pedidos_csv"]["compression"]
        header_pedidos = config["file_options"]["pedidos_csv"]["header"]
        separator_pedidos = config["file_options"]["pedidos_csv"]["sep"]

        try:
            pedidos = self.data_handler.load_pedidos(
                path=path_pedidos,
                compression=compression_pedidos,
                header=header_pedidos,
                sep=separator_pedidos,
            )
        except Exception as e:
            logger.error(f"Problemas ao carregar dados de pedidos: {e}")
            return

        print("Adicionando a coluna valor_total")
        pedidos = self.transformer.add_valor_total_pedidos(pedidos)
        pedidos.show(5, truncate=False)

        print("Fazendo a junção dos dataframes pedidos e pagamento")
        # Faz o Join entre pagamentos filtrado e pedidos
        pedidos_pagamentos = self.transformer.join_pagamentos_pedidos(
            pagamentos_selecionado, pedidos
        )
        pedidos_pagamentos.show(20, truncate=False)

        print("Selecionando colunas")
        # Seleciona colunas: Identificador do pedido, Estado (UF), Forma de pagamento, Valor total do pedido e Data do pedido
        pedidos_pagamentos_selecionado = self.transformer.get_select(pedidos_pagamentos)

        print("Filtrando e ordenando dados de 2025 ")
        # Filtra dados de 2025 e ordena por  uf, forma de pagamento e data de criação do pedido
        pedidos_pagamentos_final = self.transformer.get_filter_order(
            pedidos_pagamentos_selecionado
        )
        pedidos_pagamentos_final.show(20, truncate=False)

        print("Escrevendo o resultado em parquet")
        path_output = config["paths"]["output"]

        try:
            self.data_handler.write_parquet(
                df=pedidos_pagamentos_final, path=path_output
            )
        except Exception as e:
            logger.error(f"Problemas ao escrever dados: {e}")
            return
