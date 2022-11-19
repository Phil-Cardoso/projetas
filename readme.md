# Projetas

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

	(Priorize o uso de SQL para esta parte):
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
	  
	  - para os dados disponibilizados teria como buscar no nome do arquivo
	  - para a api, a mesma recebe parametros de query possibilitando tal resultado
	
	Justifique em cada etapa sobre a escalabilidade da tecnologia utilizada.
	  
	  - Foi feito uso de Pyspark, pois o mesmo trabalha com computação distribuida nos nós do cluster, com essa informação entramos no que se dita à escalabilidade, pensando em um ambiemte cloud (AWS EMR) poderia se manter habilitado o auto-scaling para spikes no cluster quando nescessário
	  
	Justifique as camadas utilizadas durante o processo de ingestão até a disponibilização dos dados.
	  
	  - bronze: dado como ele é, apenas um alteração, salvo como parquet
	  - silver: limpeza e normalização de dados, bem como criação de regras de negócio
	  - gold: disponibilização ao usuario final como view, anonimizando dados sensiveis

Observações:
Notebooks Jupyter - OPCIONAL
Google Colab - OPCIONAL
PYTHON e PYSPARK - OBRIGATORIO 
Disponibilizacao GIT - OBRIGATORIO
*Pode incluir comentários sobre a abordagem de extração/transformação que você está fazendo*


Questão Bônus!
Finalmente, este processo deverá ser automatizado usando a ferramenta de orquestração
de workflow Apache Airflow. Escreva uma DAG para levando em conta as
características de uso da base. Todos os passos do processo ETL devem ser listados como tasks e orquestrados de forma
otimizada, porém não é necessário implementar o código chamado em cada uma das tasks.
Foque em mostrar o fluxo de tasks e as estruturas básicas de uma DAG