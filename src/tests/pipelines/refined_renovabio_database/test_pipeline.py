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

    def test_normalize_object_columns(self):
        """
        Test normalize_object_columns function
        """

        dummy = pd.DataFrame(
            {
                'STRING_COLUMN': [
                    'IMPACTO BIOENERGIA ALAGOAS S.A.\u200b - Teotônio Vilela - AL',
                    'Santa Cruz Açúcar e Álcool Ltda. - Santa Cruz Cabrália - BA',
                    'Etanol combustível de primeira geração – cana-de-açúcar',
                    'BENRI CLASSIFICAÇÃO DA PRODUÇÃO DE AÇÚCAR\u200b E ETANOL LTDA.',
                    '44.836.856/0001-77',
                    '48610.219471/2019-56',
                ]
            }
        )

        expected = pd.DataFrame(
            {
                'STRING_COLUMN': [
                    'IMPACTO BIOENERGIA ALAGOAS S.A. - TEOTONIO VILELA - AL',
                    'SANTA CRUZ ACUCAR E ALCOOL LTDA. - SANTA CRUZ CABRALIA - BA',
                    'ETANOL COMBUSTIVEL DE PRIMEIRA GERACAO CANA-DE-ACUCAR',
                    'BENRI CLASSIFICACAO DA PRODUCAO DE ACUCAR E ETANOL LTDA.',
                    '44.836.856/0001-77',
                    '48610.219471/2019-56'
                ]
            }
        )

        actual = utils.normalize_object_columns(dummy, ['STRING_COLUMN'])
        message = "normalize_object_columns function is not working"
        assert actual.equals(expected), message

    def test_normalize_int_columns(self):
        """
        Test normalize_int_columns function
        """

        dummy = pd.DataFrame(
            {
                'INT_COLUMN': [
                    '44.836.856/0001-77',
                    '00.738.822/0002-55',
                    '48610.206100/2020-48',
                    '48610.200428/2020-51'
                ],
            }
        )

        expected = pd.DataFrame(
            {
                'INT_COLUMN': [
                    44836856000177,
                    738822000255,
                    48610206100202048,
                    48610200428202051
                ],
            }
        )

        actual = utils.normalize_int_columns(dummy, ['INT_COLUMN'])
        message = "normalize_int_columns function is not working"
        assert actual.equals(expected), message