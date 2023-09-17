# Usinas de biocombustível brasileiras

![Python version](https://img.shields.io/badge/python-3.9%20-blue.svg)
[![CI](https://github.com/daniel64bit/biofuel_brazil_plants/actions/workflows/ci_pipeline.yaml/badge.svg)](https://github.com/daniel64bit/biofuel_brazil_plants/actions/workflows/ci_pipeline.yaml)
[![codecov](https://codecov.io/gh/daniel64bit/biofuel_brazil_plants/graph/badge.svg?token=JA2JPILM8F)](https://codecov.io/gh/daniel64bit/biofuel_brazil_plants)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/daniel64bit/biofuel_brazil_plants/blob/main/LICENSE.md)

## Visão Geral

Esse projeto tem por objetivo a extração, refinamento e geocodificação de dados sobre as usinas de biocombustível brasileiras, obtidos por meio de `Certificados da Produção Eficiente de Biocombustíveis` emitidos por firmas inspetoras credenciadas no [RenovaBio](https://www.gov.br/anp/pt-br/assuntos/renovabio), aprovados pela [Agência Nacional do Petróleo, Gás Natual e Biocombustíveis (ANP)](https://www.gov.br/anp/pt-br).

Os dados gerados após todos os processamentos podem ser utilizados em análises geoespaciais, com certa margem de erro em relação à localização das usinas.

O pipeline de dados foi construído utilizando [kedro 0.18.12](https://kedro.readthedocs.io/en/stable/).

## Funcionalidades Principais

1. Extração de dados brutos de certificados de produção eficiente de biocombustíveis emitidos pela ANP;
2. Refinamento dos dados brutos, com padronização de tipos de dados e sua disposição;
3. Geocodificação dos dados refinados utilizando [Selenium](https://selenium-python.readthedocs.io/index.html) e [Google Maps](https://www.google.com.br/maps/) em desenvolvimento.
4. Visualização dos dados geocodificados em mapa, utilizando [Folium](https://python-visualization.github.io/folium/).

## Pipeline de dados

![pipeline](docs/image/biofuel_plants_pipeline.png)

## Requisitos

As bibliotecas necessárias para a execução do projeto estão listadas no arquivo `src/requirements.txt`. Para instalá-las, utilize o comando:

```
pip install -r src/requirements.txt
```

Além disso, é necessário ter a última versão do [geckodriver](https://github.com/mozilla/geckodriver/releases/) em um diretório conhecido. 

## Como executar o pipeline

Para executar o projeto, utilize o comando:

```
kedro run
```

Para executar uma pipeline específica, utilize o comando:
```
kedro run --pipeline <nome-da-pipeline>
```

## Contato

LinkedIn: [Daniel Rodrigues](https://www.linkedin.com/in/danielrod147/)
