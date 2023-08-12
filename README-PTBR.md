# Usinas de biocombustível brasileiras

[![Python version](https://img.shields.io/badge/python-3.9%20%7C%20-blue.svg)]
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/daniel64bit/biofuel_brazil_plants/blob/main/LICENSE.md)

## Visão Geral

Esse projeto tem por objetivo a extração, refinamento e geocodificação de dados sobre as usinas de biocombustível brasileiras, obtidos por meio de `Certificados da Produção Eficiente de Biocombustíveis` emitidos por firmas inspetoras credenciadas no [RenovaBio](https://www.gov.br/anp/pt-br/assuntos/renovabio), aprovados pela [Agência Nacional do Petróleo, Gás Natual e Biocombustíveis (ANP)](https://www.gov.br/anp/pt-br).

Os dados gerados após todos os processamentos podem ser utilizados em análises geoespaciais, com certa margem de erro em relação à localização das usinas.

O pipeline de dados foi construído utilizando [kedro 0.18.12](https://kedro.readthedocs.io/en/stable/).