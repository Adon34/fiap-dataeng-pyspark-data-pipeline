# src/config/settings.py
import yaml
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    """
    Responsável pelo carregamento de configurações da aplicação.
    """
    
    @staticmethod
    def get_config(path: str = "config/settings.yaml") -> dict:
        """
        Carrega um arquivo de configuração YAML.
        
        :param path: Caminho do arquivo YAML.
        :return: Dicionário com as configurações.
        """
        try:
            with open(path, "r") as file:
                config = yaml.safe_load(file)
                logger.info(f"Configuração carregada com sucesso de: {path}")
                return config
        except FileNotFoundError:
            logger.error(f"Arquivo de configuração não encontrado: {path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Erro ao fazer parse do YAML: {e}")
            raise
