"""
Test file for pipeline 'refined_renovabio_database'
generated using Kedro 0.18.12.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html
"""

import pandas as pd
from biofuel_brazil_plants.utils import utils


class TestNormalizer:
    def test_normalize_header(self):
        """
        Test normalize_header function
        """
        dummy = pd.DataFrame(
            columns=[
                "Unnamed: 0",
                "Razão Social - Cidade - UF",
                "CNPJ",
                "Processo de Certificação",
                "Biocombustível",
                "Rota",
                "Nota de Eficiência Energético-Ambiental (gCO2eq/MJ)",
                "Volume elegível (%)",
                "Fator para emissão \nde CBIO (tCO2eq/L) *",
                "Litros/CBIO",
                "Data de Aprovação pela ANP",
                "Validade",
                "Firma Inspetora",
                "Endereço Emissor Primário",
                "Unnamed: 14",
            ]
        )

        expected = pd.DataFrame(
            columns=[
                "RAZAO_SOCIAL-CIDADE-UF",
                "CNPJ",
                "PROCESSO_DE_CERTIFICACAO",
                "BIOCOMBUSTIVEL",
                "ROTA",
                "NOTA_DE_EFICIENCIA_ENERGETICO-AMBIENTAL_GCO2EQ/MJ",
                "VOLUME_ELEGIVEL_%",
                "FATOR_PARA_EMISSAO_DE_CBIO_TCO2EQ/L",
                "LITROS/CBIO",
                "DATA_DE_APROVACAO_PELA_ANP",
                "VALIDADE",
                "FIRMA_INSPETORA",
                "ENDERECO_EMISSOR_PRIMARIO",
            ]
        )

        actual = utils.normalize_header(dummy)
        assert actual.equals(expected), "normalize_header function is not working"
