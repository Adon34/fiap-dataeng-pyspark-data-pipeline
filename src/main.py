# src/main.py
import logging
import sys
from config.settings import ConfigLoader
from session.spark_session import SparkSessionManager
from pipeline.pipeline import Pipeline

class ETLApplication:
    """
    Classe principal que encapsula a execução do Pipeline ETL.
    Atua como 'Composition Root'.
    """

    def __init__(self):
        self.logger = self._configure_logging()
        self.spark = None

    def _configure_logging(self):
        """Método privado para configurar e retornar o logger."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[
                logging.FileHandler("dataeng-pyspark-poo.log"),
                logging.StreamHandler(sys.stdout),
            ],
        )
        logger = logging.getLogger(__name__)
        logger.info("Logging configurado com sucesso.")
        return logger

    def run(self):
        """Método público para iniciar a execução da aplicação."""
        try:
            # 1. Carregar Configuração (Usando a nova classe estática)
            config = ConfigLoader.get_config()
            app_name = config["spark"]["app_name"]
            
            self.logger.info(f"Iniciando aplicação: {app_name}")

            # 2. Inicialização da sessão Spark
            self.spark = SparkSessionManager.get_spark_session(app_name=app_name)

            # 3. Injeção de Dependência e Execução
            pipeline = Pipeline(self.spark)
            pipeline.run(config=config)
            
            self.logger.info("Pipeline executado com sucesso.")

        except Exception as e:
            self.logger.error(f"Ocorreu um erro inesperado: {e}", exc_info=True)
            sys.exit(1) # Sai com código de erro
            
        finally:
            # 4. Finalização segura
            if self.spark:
                self.spark.stop()
                self.logger.info("Sessão Spark finalizada.")

# Entry Point (Ponto de Entrada)
if __name__ == "__main__":
    # Instancia a classe da aplicação e roda
    app = ETLApplication()
    app.run()