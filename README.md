**Pipeline de Dados em Ecossistema de Comércio Eletrônico**
=====================================
Um projeto para explorar os diversos pipelides de dados presentes em um ecossistema de e-commerce.

### Propósito

Este projeto tem o objetivo de fornecer um ambiente de experimentação com pipelines de dados. Nos permite, de maneira escalável e confiável, coletar, processar e analisar dados de comércio eletrônico, permitindo explorar a criação de insights e tomada de decisões baseadas em dados que simulam um ambiente real.

### Recursos

* Consome dados de diversas origens (estruturado e não estruturado)
* Transforma e processa dados para análise e relatórios
* Cria um painel com análises e relatórios

### Tecnologias


1. **PostgreSQL**: Banco de dados relacional robusto e escalável com suporte a SQL padrão e transações ACID.

2. **Apache Spark**: Framework de processamento distribuído para dados em batch e stream, com alta performance.

3. **Apache Kafka**: Plataforma de streaming distribuída para transmissão e processamento de dados em tempo real.

4. **Streamlit**: Biblioteca Python para criar dashboards e interfaces interativas de forma rápida e simples.

5. **Minio**: Armazenamento de objetos compatível com S3, ideal para grandes volumes de dados não estruturados.

6. **Docker**: Plataforma de contêinerização que facilita a criação, distribuição e execução de aplicações isoladas.



### Arquitetura
![Ecommerce Data Architecture](src/diagrams/ecommerce-data-pipeline-arch_v3.jpeg)





## Como Usar

Siga os passos abaixo para clonar o repositório, criar o ambiente virtual, instalar as dependências e executar o projeto:

### 1. Clone o Repositório

Para começar, clone este repositório em sua máquina local usando o comando abaixo:

```bash
git clone git@github.com/kandarpagalas/ecommerce-data-pipeline.git
```

### 2. Instale as Bibliotecas Necessárias

Certifique-se de que você tem as seguintes bibliotecas instaladas para criar e gerenciar o ambiente virtual Python:

- Python 3.x
- `venv` (incluído no Python 3.x)
- `pip` (geralmente incluído no Python 3.x)

### 3. Crie um Ambiente Virtual

Navegue até o diretório do projeto e crie um ambiente virtual Python:

```bash
python3 -m venv venv
```

### 4. Ative o Ambiente Virtual

Ative o ambiente virtual. O comando pode variar dependendo do sistema operacional que você está usando:

**Windows:**

```bash
venv\Scripts\activate
```

**Linux/MacOS:**

```bash
source venv/bin/activate
```

### 5. Instale as Dependências

Com o ambiente virtual ativado, instale todas as dependências necessárias:

```bash
pip install -r .\requirements.txt
```

### 6. Configure o Arquivo `.env`

Antes de iniciar o projeto, certifique-se de que o arquivo `template.env` esteja corretamente configurado com as variáveis de ambiente necessárias e ter realizado a renomeação para `.env`. Este arquivo está localizado em:

```bash
C:\caminho do projeto\template.env
```


### 7. Inicie os Serviços com Docker Compose

Utilize o Docker Compose para subir os serviços necessários. Substitua `\caminho_arquivo` pelo caminho do arquivo `docker-compose.yml` específico do projeto:

```bash
docker-compose -f \caminho_arquivo up -d
```

### 8. Execute a Interface Gráfica do Streamlit

Finalmente, execute a interface gráfica do Streamlit para visualizar os dados:

```bash
streamlit run \streamlit_app.py
```



### Idealizador do projeto

* [Kandarpa Galas](https://github.com/kandarpagalas/) 

### Contribuidores
* [Felipe Soares](https://github.com/felipesoaresdev/)
* [Winiston Freitas](https://github.com/winistonvf)


### Licença

Este projeto é licenciado sob a [Licença Apache 2.0](LICENSE).