# syncnosqldb
Aplicação em Python para sincronizar dados entre Elasticsearch e Cassandra

## Executando o código

Para rodar o programa, é necessário passar como parâmetro o período de tempo em segundos em que o programa irá verificar se dados novos chegaram no Cassandra ou no Elasticsearch. Por exemplo:

    python run_sync.py 10

Executando este comando o programa ficará rodando e, a cada 10 segundos vai verificar se existem novos dados e sincronizá-los entre os dois bancos.
O programa irá exibir no console o número de verificações que foram realizadas atualmente e exibe se foi realizada uma sincronização.
Para parar a execução é necessário pressionar as teclas Ctrl + c.

## Dados iniciais

Foram desenvolvidos dois scripts para popular inicialmente os dois bancos com dados.
Para o Cassandra, basta executar:

    python script_create_cassandra.py

E serão inseridos dados no banco.

Para o Elasticsearch, execute:

    python script_create_elasticsearch.py

Os dados iniciais são idênticos para os dois scripts.

## O código

Parte do código foi baseado neste projeto: https://github.com/arthurprs/Cassandra-Elasticsearch-Sync
Que tem o mesmo propósito, sincronizar dados entre Cassandra e Elasticsearch, porém de forma mais complexa e com maior número de configurações.

