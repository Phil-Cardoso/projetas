# Projetas

[Código no google colab](https://colab.research.google.com/drive/17TG3qnL-Q3OmoRCAWL5BtkcslVqlVBm1?usp=sharing)

  A Projetas irá atender um novo cliente, e você será o engenheiro de dados responsável por fazer a ingestão de dados e preparar algumas tabelas para os cientistas de dados e analistas de dados.

*Carregar os dados de VRA*

	Normalizar o cabeçalho para snake case
	Salvar estes dados
	
*Carregar dos dados de AIR_CIA*

	Normalizar o cabeçalho para snake case
	Separar a coluna 'ICAO IATA' em duas colunas, seu conteúdo está separado por espaço e pode não conter o código IATA, caso não contenha o código IATA, deixe o valor nulo.
	Salvar estes dados

*Criar nova tabela aerodromos*

	Através da API https://rapidapi.com/Active-api/api/airport-info/ trazer os aeródramos através do código ICAO presente nos dados de VRA.
	Salvar estes dados

*Criar as seguintes views*
(**Priorize o uso de SQL para esta parte**)

Para cada companhia aérea trazer a rota(origem-destino) mais utilizada com as seguintes informações:

	Razão social da companhia aérea
	Nome Aeroporto de Origem
	ICAO do aeroporto de origem
	Estado/UF do aeroporto de origem
	Nome do Aeroporto de Destino
	ICAO do Aeroporto de destino
	Estado/UF do aeroporto de destino

Para cada aeroporto trazer a companhia aérea com maior atuação no ano com as seguintes informações:

	Nome do Aeroporto
    ICAO do Aeroporto
    Razão social da Companhia Aérea
    Quantidade de Rotas à partir daquele aeroporto
    Quantidade de Rotas com destino àquele aeroporto
    Quantidade total de pousos e decolagens naquele aeroporto	

*Extras:*

Descrever qual estratégia você usaria para ingerir estes dados de forma incremental caso precise capturar esses dados a cada mes?
	  
	Criaria uma coluna com o nome ou a data dos arquivos de origem (os csv e json tem nome com uma parte da data), 
    consultaria no destino qual foi a última carga e subiria a diferença usando append. No caso da API eu consultaria 
    os dados dos novos arquivos, compararia com o log _icao, pegaria os novos e os que fossem iguais a “n encontrado”.
	
Justifique em cada etapa sobre a escalabilidade da tecnologia utilizada.
	  
	Feito com Pyspark, pois o mesmo trabalha com computação distribuída e isso permite uma escalabilidade
	  
Justifique as camadas utilizadas durante o processo de ingestão até a disponibilização dos dados.
	  
	Bronze: Dado bruto, sem alterações ou tratativas.
	Silver: Dados tratado e normalizados conforme solicitado aplicando regras de negócio.
	Gold: Dados disponibilizados para os usuários em formato de view, fazendo criptografia em dados sensíveis.

    Arquivos e API = Bronze
    ELT de VR e AIR_CIA = Silver
    Views = Gold

Observações

	Notebooks Jupyter - OPCIONAL
    Google Colab - OPCIONAL
    PYTHON e PYSPARK - OBRIGATORIO 
    Disponibilizacao GIT - OBRIGATORIO
      
    *Pode incluir comentários sobre a abordagem de extração/transformação que você está fazendo*


# **Questão Bônus**

	Finalmente, este processo deverá ser automatizado usando a ferramenta de orquestração
    de workflow Apache Airflow. Escreva uma DAG para levando em conta as
    características de uso da base. Todos os passos do processo ETL devem ser listados como tasks e orquestrados de forma
    otimizada, porém não é necessário implementar o código chamado em cada uma das tasks.
    Foque em mostrar o fluxo de tasks e as estruturas básicas de uma DAG
