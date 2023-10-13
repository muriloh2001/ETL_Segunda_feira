#ETL Microdados do INEP = Educação Superior
#Autor: Murilo
#Data: 14/08/2023

#Conectar na base do DW_INEP
import mysql.connector
import pandas as pd

#Dictionary com a config do banco para conexão
config = {
    'user':'root',
    'password':'123030',
    'host': 'localhost',
    'database':'dw_inep',
    'port': '3306'
}


try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # dados = pd.read_csv('C:/Users/Aluno/Downloads/Microdados do Censo da Educação Superior 2020/dados/MICRODADOS_CADASTRO_CURSOS_2020.CSV',sep=';', encoding='iso-8859-1')
    dados = pd.read_csv('C:/Users/USER/Desktop/Dados/Microdados do Censo da Educação Superior 2020/dados/MICRODADOS_CADASTRO_CURSOS_2020.CSV',
                        sep=';'
                        , encoding='iso-8859-1'
                        , low_memory=False)
    dados = dados.fillna('')   
     
   # #Curso 2020
    dados_curso = pd.DataFrame(dados['NO_CURSO'].unique(), columns = ['CURSO'])
    for i,r in dados_curso.iterrows():
        
        insert_statement = f"insert into dim_curso (tf_curso, curso) values({i}, '{r['CURSO']}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()
    
    ##Ano 2020
    dados_ano= pd.DataFrame(dados['NU_ANO_CENSO'].unique(), columns = ['ANO'])
    for i,r in dados_ano.iterrows():
        
        insert_statement = f"insert into dim_ano (tf_ano, ano) values({i}, '{r['ANO']}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()
    

    
    ##ies 2020
    dados_IES = pd.read_csv('C:/Users/USER/Desktop/Dados/Microdados do Censo da Educação Superior 2020/dados/MICRODADOS_CADASTRO_IES_2020.CSV'
                    ,sep=';'
                    , encoding='iso-8859-1'
                    , low_memory=False)
    dados_IES = dados_IES[['CO_IES','NO_IES']]
    
    dados_IES_curso = pd.DataFrame(dados['CO_IES'].unique(), columns = ['IES'])
    for i, r in dados_IES_curso.iterrows():
        
        #determinar o nome  da ies
        dados_IES_filtrado = dados_IES[dados_IES['CO_IES'] == r['IES']]
        filtro_dados = dados_IES_filtrado['NO_IES'].iloc[0].replace("'","")
        insert_statement = f"insert into dim_ies (tf_ies, ies) values({i}, '{filtro_dados}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()

    ##Fact matriculas 2020
    for i, r in dados.iterrows():
        
        modalidade = 'Presencial' if r['TP_MODALIDADE_ENSINO'] == 1 else 'EAD'
        dados_ies_filtrado = dados_IES[dados_IES['CO_IES'] == r['CO_IES']]
        no_ies = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", "")
        municipio = str(r['NO_MUNICIPIO']).replace("'", "")

        subquery_matriculas = f"(select {r['QT_INSCRITO_TOTAL']}) as matriculas"
        subquery_tf_ano = f"(select tf_ano from dim_ano where ano = {r['NU_ANO_CENSO']}) AS tf_ano"
        subquery_tf_modalidade = f"(select tf_modalidade from dim_modalidade where modalidade = '{modalidade}') AS tf_modalidade"
        subquery_tf_municipio = f"(select tf_municipio from dim_municipio where municipio = '{municipio}') AS tf_municipio" \
            if r['NO_MUNICIPIO'] else f"(select Null from dim_municipio where municipio = '{r['NO_MUNICIPIO']}') AS tf_municipio"
        subquery_tf_uf = f"(select tf_uf from dim_uf where uf = '{r['NO_UF']}') AS tf_uf" if r[
            'NO_UF'] else f"(select Null from dim_uf where uf = '{r['NO_UF']}') AS tf_uf"
        subquery_tf_ies = f"(select tf_ies from dim_ies where ies = '{no_ies}') AS tf_ies"
        subquery_tf_curso = f"(select tf_curso from dim_curso where curso = '{r['NO_CURSO']}') AS tf_curso"

        insert_statement = f"""insert into fact_matriculas (matricula, tf_curso, tf_ano, tf_modalidade, tf_municipio, tf_uf, tf_ies)
                                select DISTINCT * from
                                {subquery_matriculas},{subquery_tf_curso},{subquery_tf_ano},{subquery_tf_modalidade},
                                {subquery_tf_municipio},{subquery_tf_uf},{subquery_tf_ies};
                            """
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()
        
        
        
################################## CSV 2021 ##################################################################

    dados = pd.read_csv('C:/Users/USER/Desktop/Dados2021/Microdados do Censo da Educação Superior 2021/dados/MICRODADOS_CADASTRO_CURSOS_2021.CSV',
                        sep=';'
                        , encoding='iso-8859-1'
                        , low_memory=False)
    dados = dados.fillna('')

 #UF 2021
    uf_mapping = {}

    dados_uf = pd.DataFrame(dados['NO_UF'].unique(), columns=['UF'])
    for i, r in dados_uf.iterrows():
        uf = r['UF']
        select_statement = f"select count(*) from dim_uf where uf = '{uf}'"
        cursor.execute(select_statement)
        result = cursor.fetchone()

        if result[0] == 0:
            uf_mapping[i] = uf
            print(f"UF '{uf}' não existe na tabela dim_uf. Será inserida posteriormente.")
        else:
            print(f"UF '{uf}' já existe na tabela dim_uf.")

# Após o loop, insire as UF que não existem na tabela dim_uf
    for i, uf in uf_mapping.items():
        insert_statement = f"insert into dim_uf (tf_uf, uf) values ({i}, '{uf}')"
        cursor.execute(insert_statement)
        conn.commit()
             
 #Municipio 2021            
    municipio_mapping = {}

    dados_municipio = pd.DataFrame(dados['NO_MUNICIPIO'].unique(), columns=['MUNICIPIO'])
    for i, r in dados_municipio.iterrows():
        municipio = r['MUNICIPIO'].replace("'", " ")
        select_statement = f"select count(*) from dim_municipio where municipio = '{municipio}'"
        cursor.execute(select_statement)
        result = cursor.fetchone()

        if result[0] == 0:
            municipio_mapping[i] = municipio
            print(f"Município '{municipio}' não existe na tabela dim_municipio. Será inserido posteriormente.")
        else:
            print(f"Município '{municipio}' já existe na tabela dim_municipio.")

    # Após o loop, insire os municípios que não existem na tabela dim_municipio
    for i, municipio in municipio_mapping.items():
        insert_statement = f"insert into dim_municipio (tf_municipio, municipio) values ({i}, '{municipio}')"
        cursor.execute(insert_statement)
        conn.commit()

 #ies 2021

    dados_IES = pd.read_csv('C:/Users/USER/Desktop/Dados2021/Microdados do Censo da Educação Superior 2021/dados/MICRODADOS_CADASTRO_IES_2021.CSV'
                    ,sep=';'
                    , encoding='iso-8859-1'
                    , low_memory=False)
    dados_IES = dados_IES[['CO_IES','NO_IES']]
    

    ies_mapping = {}

    dados_ies_curso = pd.DataFrame(dados['CO_IES'].unique(), columns=['IES'])
    for i, r in dados_ies_curso.iterrows():
        # Determinar o nome da instituição de ensino
        dados_ies_filtrado = dados_IES[dados_IES['CO_IES'] == r['IES']]
        dados_filtrados = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", " ")
        select_statement = f"select count(*) from dim_ies where ies = '{dados_filtrados}'"
        cursor.execute(select_statement)
        result = cursor.fetchone()

        if result[0] == 0:
            ies_mapping[i] = dados_filtrados
            print(f"Instituição de ensino '{dados_filtrados}' não existe na tabela dim_ies. Será inserida posteriormente.")
        else:
            print(f"Instituição de ensino '{dados_filtrados}' já existe na tabela dim_ies.")

    # Após o loop, insire as instituições de ensino que não existem na tabela dim_ies
    for i, ies in ies_mapping.items():
        insert_statement = f"insert into dim_ies (tf_ies, ies) values ({i}, '{ies}')"
        cursor.execute(insert_statement)
        conn.commit()
             
 #Modalidade de ensino 2021            
    modalidade_mapping = {
    1: 'Presencial',
    2: 'EAD'
}

    dados_modalidade = pd.DataFrame(dados['TP_MODALIDADE_ENSINO'].unique(), columns=['MODALIDADE'])
    for i, r in dados_modalidade.iterrows():
        modalidade = r['MODALIDADE']

        if modalidade in modalidade_mapping:
            descricao_modalidade = modalidade_mapping[modalidade]
            select_statement = f"select count(*) from dim_modalidade where modalidade = '{descricao_modalidade}'"
            cursor.execute(select_statement)
            result = cursor.fetchone()

            if result[0] == 0:
                insert_statement = f"insert into dim_modalidade (tf_modalidade, modalidade) values ('{modalidade}', '{descricao_modalidade}')"
                cursor.execute(insert_statement)
                conn.commit()
            else:
                print(f'Modalidade {descricao_modalidade} já existe na tabela dim_modalidade.')

                 
 #Curso 2021    
    curso_mapping = {}

    dados_curso = pd.DataFrame(dados['NO_CURSO'].unique(), columns=['CURSO'])
    for i, r in dados_curso.iterrows():
        curso = r['CURSO'].replace("'", " ")
        select_statement = f"select count(*) from dim_curso where curso = '{curso}'"
        cursor.execute(select_statement)
        result = cursor.fetchone()

        if result[0] == 0:
            curso_mapping[i] = curso
            print(f"Curso '{curso}' não existe na tabela dim_curso. Será inserido posteriormente.")
        else:
            print(f"Curso '{curso}' já existe na tabela dim_curso.")

    # Após o loop, insire os cursos que não existem na tabela dim_curso
    for i, curso in curso_mapping.items():
        insert_statement = f"insert into dim_curso (tf_curso, curso) values ({i}, '{curso}')"
        cursor.execute(insert_statement)
        conn.commit() 
             
 #Ano 2021      
    ano_mapping = {}

    dados_ano = pd.DataFrame(dados['NU_ANO_CENSO'].unique(), columns=['ANO'])
    for i, r in dados_ano.iterrows():
        ano = r['ANO']
        select_statement = f"select count(*) from dim_ano where ano = {ano}"
        cursor.execute(select_statement)
        result = cursor.fetchone()

        if result[0] == 0:
            ano_mapping[i] = ano
            print(f"Ano '{ano}' não existe na tabela dim_ano. Será inserido posteriormente.")
        else:
            print(f"Ano '{ano}' já existe na tabela dim_ano.")

    # Após o loop, insire os anos que não existem na tabela dim_ano
    for i, ano in ano_mapping.items():
        insert_statement = f"insert into dim_ano (tf_ano, ano) values ({i + 1}, {ano})"
        cursor.execute(insert_statement)
        conn.commit()    
             
 #Fact matriculas 2021
    for i, r in dados.iterrows():
         modalidade = 'Presencial' if r['TP_MODALIDADE_ENSINO'] == 1 else 'EAD'
         dados_ies_filtrado = dados_IES[dados_IES['CO_IES'] == r['CO_IES']]
         no_ies = dados_ies_filtrado['NO_IES'].iloc[0].replace("'", "")
         municipio = str(r['NO_MUNICIPIO']).replace("'", "")

         subquery_matriculas = f"(select {r['QT_INSCRITO_TOTAL']}) as matriculas"
         subquery_tf_ano = f"(select tf_ano from dim_ano where ano = {r['NU_ANO_CENSO']}) AS tf_ano"
         subquery_tf_modalidade = f"(select tf_modalidade from dim_modalidade where modalidade = '{modalidade}') AS tf_modalidade"
         subquery_tf_municipio = f"(select tf_municipio from dim_municipio where municipio = '{municipio}') AS tf_municipio" \
             if r['NO_MUNICIPIO'] else f"(select Null from dim_municipio where municipio = '{r['NO_MUNICIPIO']}') AS tf_municipio"
         subquery_tf_uf = f"(select tf_uf from dim_uf where uf = '{r['NO_UF']}') AS tf_uf" if r[
             'NO_UF'] else f"(select Null from dim_uf where uf = '{r['NO_UF']}') AS tf_uf"
         subquery_tf_ies = f"(select tf_ies from dim_ies where ies = '{no_ies}') AS tf_ies"
         subquery_tf_curso = f"(select tf_curso from dim_curso where curso = '{r['NO_CURSO']}') AS tf_curso"

         insert_statement = f"""insert into fact_matriculas (matricula, tf_curso, tf_ano, tf_modalidade, tf_municipio, tf_uf, tf_ies)
                                 select DISTINCT * from
                                 {subquery_matriculas},{subquery_tf_curso},{subquery_tf_ano},{subquery_tf_modalidade},
                                 {subquery_tf_municipio},{subquery_tf_uf},{subquery_tf_ies};
                             """
         print(insert_statement)
         cursor.execute(insert_statement)
         conn.commit() 
                 
except Exception as e:
    print(e)
finally:
    if conn is not None and conn.is_connected():
        conn.close()
        print('Conexão fechada.')