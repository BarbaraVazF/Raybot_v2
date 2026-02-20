import os
import sys
import asyncio
from dotenv import load_dotenv
load_dotenv()
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langgraph.prebuilt import create_react_agent
from sqlalchemy import create_engine
from langchain_core.messages import SystemMessage, HumanMessage
import datetime
from langgraph.checkpoint.memory import MemorySaver
import tools as kpi_tools
from prompt import SYSTEM_PROMPT_TEXT

# 1. Configuração do Banco de Dados
DB_PATH = "sqlite:///db_raybot"
engine = create_engine(DB_PATH)

# Configura a engine globalmente no tools.py
kpi_tools.set_db_engine(engine)

db = SQLDatabase(engine)

# 2. Configuração do Modelo
if not os.getenv("OPENAI_API_KEY"):
    print("❌ ERRO: A chave OPENAI_API_KEY não foi encontrada no arquivo .env")
    sys.exit(1)

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

# 3. Preparação das Ferramentas

# A: Ferramentas de SQL
sql_toolkit = SQLDatabaseToolkit(db=db, llm=llm)
sql_tools = sql_toolkit.get_tools()

# B: Suas Ferramentas de KPI
custom_tools = [
    kpi_tools.calcular_icmq,
    kpi_tools.calcular_idf,
    kpi_tools.calcular_imp,
    kpi_tools.calcular_oemcp,
    kpi_tools.calcular_oempp,
    kpi_tools.calcular_preventivas_liquidadas,
    kpi_tools.calcular_km_falhas,
    kpi_tools.calcular_qetg,
    kpi_tools.calcular_qett,
    kpi_tools.calcular_cdtdm,
    kpi_tools.calcular_caiefo,
    kpi_tools.calcular_qva,
    kpi_tools.calcular_qvv,
    kpi_tools.calcular_tic,
    kpi_tools.calcular_to,
    kpi_tools.calcular_topp,
    kpi_tools.calcular_tia,
    kpi_tools.calcular_iavlit,
    kpi_tools.calcular_pcv,
    kpi_tools.calcular_ioalo,
    kpi_tools.calcular_indoa,
    kpi_tools.analisar_evolucao_kpi,
    kpi_tools.consultar_meta_indicador,
    kpi_tools.calcular_kpi_por_mes
]

all_tools = custom_tools + sql_tools

hoje = datetime.datetime.now().strftime("%d/%m/%Y")

# 4. Criação do Agente
agent_executor = create_react_agent(llm, tools=all_tools)

# 5. Execução
async def main():
    print("🤖 Raybot Iniciado. Digite 'sair' para encerrar.")
    
    # Defina o tempo máximo de espera (em segundos). Ajuste conforme necessário.
    TEMPO_MAXIMO_SEGUNDOS = 45

    while True:
        user_input = input("\nPergunte: ")
        if user_input.lower() in ["sair", "exit", "quit"]:
            break
        
        try:
            hoje_atualizado = datetime.datetime.now().strftime("%d/%m/%Y")
            prompt_formatado = SYSTEM_PROMPT_TEXT.replace("{hoje}", hoje_atualizado)
            messages = [
                SystemMessage(content=prompt_formatado),
                HumanMessage(content=user_input)
            ]
            
            inputs = {"messages": messages}
            
            # Executa a chamada ao agente dentro de um executor de thread
            try:
                # O wait_for corta a execução de verdade se passar do tempo
                result = await asyncio.wait_for(
                    agent_executor.ainvoke(inputs), 
                    timeout=TEMPO_MAXIMO_SEGUNDOS
                )
                
                resposta_final = result["messages"][-1].content
                print(f"\n📢 Raybot: {resposta_final}")
                
            except asyncio.TimeoutError:
                print(f"\n📢 Raybot: Puxa, essa consulta está demorando mais do que o esperado ({TEMPO_MAXIMO_SEGUNDOS} segundos). Você poderia tentar simplificar a pergunta ou reduzir o período analisado?")
            
        except Exception as e:
            # O ideal é logar o erro 'e' em um arquivo, mas para o usuário mantemos amigável
            print(f"\n📢 Raybot: Infelizmente, não foi possível responder à pergunta neste momento.")

if __name__ == "__main__":
    asyncio.run(main())