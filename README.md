Para rodar:

docker build -t socrata-local .

docker run -v ~/Desktop/socrata-data:/app/data --name socrata-local-container socrata-local


Se mudar o nome do arquivo, mudar o nome que está na Dockerfile
