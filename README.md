Para rodar:

docker build -t socrata-local .

docker run -v ~/Desktop/socrata-data:/app/data --name socrata-local-container socrata-local

Endpoints Disponíveis

    Criar arquivo CSV vazio:
        Endpoint: /accidents/emptyFile
        Método: GET
        Descrição: Cria um arquivo CSV vazio na pasta de dados.

    Baixar o arquivo CSV:
        Endpoint: /accidents/downloadFile
        Método: GET
        Descrição: Baixa o arquivo CSV com os dados atuais.

    Testar a execução do Job:
        Endpoint: /accidents/testJob
        Método: GET
        Descrição: Executa o job manualmente para buscar dados das últimas 24 horas e atualizar o CSV.

    Obter todos os dados:
        Endpoint: /accidents/data
        Método: GET
        Descrição: Retorna todos os dados armazenados no CSV.

    Obter dados por ID:
        Endpoint: /accidents/data/<record_id>
        Método: GET
        Descrição: Retorna os dados de um registro específico pelo ID.

    Deletar dados por ID:
        Endpoint: /accidents/data/<record_id>
        Método: DELETE
        Descrição: Deleta um registro específico pelo ID.

    Verificar Conexão:
        Endpoint: /accidents
        Método: GET
        Descrição: Verifica a conexão com o servidor.

Se mudar o nome do arquivo, mudar o nome que está na Dockerfile
