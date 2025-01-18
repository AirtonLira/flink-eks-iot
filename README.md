# Projeto: Integração CoinGecko, Kafka, Flink e MinIO

## Objetivos do Projeto

Este projeto irá:

- Consumir dados da API CoinGecko a cada minuto.
- Enviar os dados para um tópico Kafka.
- Processar os dados com Flink.
- Converter os dados para formato Parquet.
- Armazenar os arquivos Parquet no MinIO.

## Pontos Importantes

- O MinIO estará acessível em [http://localhost:9001](http://localhost:9001) (console web).
  - **Usuário**: `minioadmin`
  - **Senha**: `minioadmin`
- O Flink UI estará disponível em [http://localhost:8081](http://localhost:8081).
- Os dados são coletados a cada minuto para respeitar os limites da API do CoinGecko.
- Os arquivos Parquet são nomeados com timestamp para evitar sobrescrita.