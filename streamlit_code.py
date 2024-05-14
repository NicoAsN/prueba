import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

session = get_active_session()
        
selectbox_sql = """
                SELECT DISTINCT CANTANTE
                FROM DEMOSDB.REFINED_SCH.MUSIC_CLEAN
                ORDER BY CANTANTE ASC;
                """
sql2 = """
        SELECT
            DISTINCT CANCIONES   
        FROM
            DEMOSDB.REFINED_SCH.MUSIC_CLEAN
        WHERE CANTANTE = '{evt_typ}'
        ORDER BY CANCIONES ASC
        """
selectbox_df = session.sql(selectbox_sql).collect()

selected_artist_song = """
        SELECT
            *   
        FROM
            DEMOSDB.REFINED_SCH.MUSIC_CLEAN
        WHERE
            CANTANTE = '{evt_typ}' 
            AND CANCIONES = '{evt_typ2}' 
        """


with st.sidebar:
    "# Snowflake Cortex"

    #####   HOME   #####
    def home():
        st.markdown('# Spotify Song 🎸🎧🎶')
        st.write("""
                En esta demo nos centraremos en utilizar las nuevas implementaciones de Snowflake ❄ 
                utilizando los algoritmos de procesamiento de lenguaje natural, por lo cual tendremos
                que seleccionar al artista🕺🏼💃🏼 y uno de sus éxitos 🎵🎶 para poder analizarlo con los distintos
                algoritmos que tendremos gracias a Cortex 🖥️.
                """)
        st.markdown('## Resumen de los algoritmos de Cortex 🤖💻')
        st.markdown('### Complete ')
        st.write("""
                La función COMPLETE que sigue la instrucción genera una respuesta utilizando el
                modelo de lenguaje que elija. En el caso de uso más simple, el mensaje es 
                una sola cadena. También puede proporcionar una conversación que incluya 
                múltiples indicaciones y respuestas para el uso de estilo de chat interactivo, 
                y en esta forma de la función también puede especificar opciones de 
                hiperparámetros para personalizar el estilo y el tamaño de la salida.
                """)
        st.write("""
                La función COMPLETE es compatible con los siguientes modelos. 
                Los diferentes modelos pueden tener diferentes costos y cuotas:
                """)
        st.write("""
                * mistral-large
                * mixtral-8x7b
                * llama2-70b-chat
                * mistral-7b
                * gemma-7b
                """)
        st.markdown('### Extract Answer')
        st.write("""
                La función EXTRACT_ANSWER extrae una respuesta a una pregunta determinada de un
                documento de texto. El documento puede ser un documento en inglés sin formato 
                o una representación de cadena de un objeto de datos semiestructurado (JSON).
                """)
        st.markdown('### Sentiment')
        st.write("""
                La función SENTIMENT devuelve el sentimiento como una puntuación entre -1 y 1 
                (siendo -1 el más negativo y 1 el más positivo, con valores alrededor de 0 
                neutro) para el texto de entrada en inglés dado.
                """)
        st.markdown('### Resume')
        st.write("""
                La función SUMMARIZE devuelve un resumen del texto en inglés dado.
                """)
        st.markdown('### Translate')
        st.write("""
                La función TRANSLATE traduce el texto del idioma de origen indicado o 
                detectado a un idioma de destino.
                """)
    
    
    #####   COMPLETE FUNCTION   #####
    def complete():
        st.markdown("# Complete 🤖")
        st.sidebar.markdown("### Dada una pregunta, devuelve una respuesta que completa la pregunta. Esta función acepta una sola pregunta o una conversación con varias preguntas y respuestas.")

        #CREATE THE SELECT BOX; STORE SELECTED VALUE IN VAR_EVT_TYPE
        var_evt_type=st.selectbox(label="Seleccione un artista:",options=selectbox_df)
        
        #FORMAT SQL STRING WITH VARIABLE
        var_song = sql2.format(evt_typ=var_evt_type)
        
        #QUERY SNOWFLAKE
        df_song = session.sql(var_song).collect()
        var_evt_type_2=st.selectbox(label="Selecciona una canción:",options=df_song)
        
        #FORMAT SQL STRING WITH VARIABLE
        sql = selected_artist_song.format(evt_typ=var_evt_type,evt_typ2=var_evt_type_2)
        
        #QUERY SNOWFLAKE
        df = session.sql(sql).collect()
        
        if 'clicked' not in st.session_state:
            st.session_state.clicked = False
         
        def click_button():
            st.session_state.clicked = True
        #st.button("Reset", type="primary")
        if st.button('Generar playlist'):
            st.write('Generando la respuesta con IA!')
            sql_cortext_complete = """
            SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', 'Genera una playlist con canciones de cualquier artista relacionadas a esta cancion: {evt_typ2} de {evt_typ} ') AS snowflake 
            """
            #FORMAT SQL STRING WITH VARIABLE
            sql = sql_cortext_complete.format(evt_typ=var_evt_type,evt_typ2=var_evt_type_2)
            #QUERY SNOWFLAKE
            df_cortext_complete = session.sql(sql).collect()
            #WRITE TO SCREEN

            df = pd.DataFrame(df_cortext_complete)
            st.text_area("Respuesta", df.loc[0, 'SNOWFLAKE'], height=300)
 
        else:
            st.write('Selecciona la canción')


    #####   EXTRACT ANSWER FUNCTION   #####
    def extract_answer():
        st.markdown("# Extract answer ❄️")
        st.sidebar.markdown("### Dada una pregunta y datos no estructurados, devuelve la respuesta a la pregunta si puede encontrarse en los datos.")

        informacion = st.text_input('Ingrese la informacion', '')
        pregunta = st.text_input('Ingrese la pregunta', '')

        def click_button():
            st.session_state.clicked = True
        st.button("Reset", type="primary")
        #st.button("Reset", type="primary")
        if st.button('Extraer informacion'):
            sql_cortext_extract_answer = """
            SELECT
                TO_JSON(SNOWFLAKE.CORTEX.EXTRACT_ANSWER($$'{informacion1}'$$, '{pregunta1}')) AS respuesta,
                SNOWFLAKE.CORTEX.TRANSLATE(JSON_EXTRACT_PATH_TEXT(respuesta,'[0].answer'), 'en', 'es') as extraer,
                JSON_EXTRACT_PATH_TEXT(respuesta,'[0].score') as score;
            """
            #FORMAT SQL STRING WITH VARIABLE
            sql = sql_cortext_extract_answer.format(informacion1=informacion,pregunta1=pregunta)
    
            df_cortext_extract_answer = session.sql(sql).collect()
    
            df = pd.DataFrame(df_cortext_extract_answer)
            st.text_area("",df.loc[0, 'EXTRAER'] + ' Score: ' + df.loc[0,'SCORE'], height= 10)
 
        else:
            st.write('ingresa tu pregunta')
        

    #####   SENTIMENT FUNCTION   #####
    def sentiment():
        st.markdown("# Sentiment 😁😭")
        st.sidebar.markdown("### Devuelve una puntuación de sentimiento, de -1 a 1, que representa el sentimiento positivo o negativo detectado del texto dado.")
        
        #CREATE THE SELECT BOX; STORE SELECTED VALUE IN VAR_EVT_TYPE
        var_evt_type=st.selectbox(label="Seleccione un artista:",options=selectbox_df)
        
        #FORMAT SQL STRING WITH VARIABLE
        var_song = sql2.format(evt_typ=var_evt_type)
        #QUERY SNOWFLAKE
        df_song = session.sql(var_song).collect()
        var_evt_type_2=st.selectbox(label="Selecciona una canción:",options=df_song)

        #FORMAT SQL STRING WITH VARIABLE
        sql = selected_artist_song.format(evt_typ=var_evt_type,evt_typ2=var_evt_type_2)
        #QUERY SNOWFLAKE
        df = session.sql(sql).collect()
         
        #BUILD SQL COMMAND
        sql_cortext_sentiment = """
        SELECT
            CANTANTE as Artista,
            CANCIONES as Cancion,
            LETRA AS Letra,
            SNOWFLAKE.CORTEX.SENTIMENT(LETRA) AS PUNTAJE,
            CASE
                  WHEN SNOWFLAKE.CORTEX.SENTIMENT(LETRA) <= '-0.33'
                     THEN 'Depresiva 🤕'
                  WHEN SNOWFLAKE.CORTEX.SENTIMENT(LETRA) >= '0.33' THEN 'Feliz 😄'
                  ELSE 'Neutral 😐'
             END AS Sentimiento
        FROM
            DEMOSDB.REFINED_SCH.MUSIC_CLEAN
        WHERE
            Artista = '{evt_typ}'
            AND Cancion = '{evt_typ2}' 
        """
         
        st.subheader("Analisis de sentimiento:")
        
        #FORMAT SQL STRING WITH VARIABLE
        sql = sql_cortext_sentiment.format(evt_typ=var_evt_type,evt_typ2=var_evt_type_2)
        #QUERY SNOWFLAKE
        df_cortext_sentiment = session.sql(sql).collect()

        df = pd.DataFrame(df_cortext_sentiment)
        st.text_area("",'La cancion '+ df.loc[0, 'CANCION'] + ' del artista ' + df.loc[0, 'ARTISTA'] + ' es una cancion ' + df.loc[0, 'SENTIMIENTO'] + ' con un score de : ' + str(round(df.loc[0, 'PUNTAJE'],2)) , height= 10)

    #####   SUMMARIZE FUNCTION   #####
    def summarize():
        st.markdown("# Summarize 📜")
        st.sidebar.markdown("### Devuelve un resumen del texto dado.")

        #CREATE THE SELECT BOX; STORE SELECTED VALUE IN VAR_EVT_TYPE
        var_evt_type=st.selectbox(label="Seleccione un artista:",options=selectbox_df)
        
        #FORMAT SQL STRING WITH VARIABLE
        var_song = sql2.format(evt_typ=var_evt_type)
        #QUERY SNOWFLAKE
        df_song = session.sql(var_song).collect()
        var_evt_type_2=st.selectbox(label="Selecciona una canción:",options=df_song)
        #BUILD SQL COMMAND
        
        #FORMAT SQL STRING WITH VARIABLE
        sql = selected_artist_song.format(evt_typ=var_evt_type,evt_typ2=var_evt_type_2)
        #QUERY SNOWFLAKE
        df = session.sql(sql).collect()
         
        #BUILD SQL COMMAND
        sql_cortext_summarize = """
        SELECT
            SNOWFLAKE.CORTEX.TRANSLATE(SNOWFLAKE.CORTEX.SUMMARIZE(LETRA), 'en','es') AS RESUMEN,
        FROM
            DEMOSDB.REFINED_SCH.MUSIC_CLEAN
        WHERE
            CANTANTE = '{evt_typ}'
            AND CANCIONES = '{evt_typ2}' 
        """
         
        st.subheader("Resumen de la canción:")
         
        #FORMAT SQL STRING WITH VARIABLE
        sql = sql_cortext_summarize.format(evt_typ=var_evt_type,evt_typ2=var_evt_type_2)
        #QUERY SNOWFLAKE
        df_cortext_summarize = session.sql(sql).collect()
         
        #WRITE TO SCREEN
        df = pd.DataFrame(df_cortext_summarize)
        st.text_area("",df.loc[0,'RESUMEN'], height= 200)


    #####   TRANSLATE FUNCTION   #####
    def translate():
        st.markdown("# Translate 👅")
        st.sidebar.markdown("### Traduce un texto dado de cualquier idioma compatible a cualquier otro.")

        var_evt_type=st.selectbox(label="Seleccione un artista:",options=selectbox_df)
      
        #FORMAT SQL STRING WITH VARIABLE
        var_song = sql2.format(evt_typ=var_evt_type)
        #QUERY SNOWFLAKE
        df_song = session.sql(var_song).collect()
        var_evt_type_2=st.selectbox(label="Selecciona una canción:",options=df_song)
      
        if 'clicked' not in st.session_state:
            st.session_state.clicked = False
         
        def click_button():
            st.session_state.clicked = True
        st.button("Reset", type="primary")
        
        if st.button('Traducir'):
            st.write('Generando la traducción con IA!')
            sql_cortext2 = """
                    SELECT
                        SNOWFLAKE.CORTEX.TRANSLATE(SUBSTRING(CANCIONES,1000),'en','es') AS traduccion
                    FROM
                        DEMOSDB.REFINED_SCH.MUSIC_CLEAN
                    WHERE
                        CANTANTE = '{evt_typ}' AND CANCIONES = '{evt_typ2}' 
            """
            #FORMAT SQL STRING WITH VARIABLE
            sql = sql_cortext2.format(evt_typ=var_evt_type,evt_typ2=var_evt_type_2)
            #QUERY SNOWFLAKE
            df_cortex_translate = session.sql(sql).collect()
            #WRITE TO SCREEN
            df = pd.DataFrame(df_cortex_translate)
            st.text_area("",df.loc[0,'TRADUCCION'], height= 200)
         
        else:
            st.write('Selecciona la canción')
         


page_names_to_funcs = {
    "HOME": home,
    "COMPLETE": complete,
    "EXTRACT ANSWER": extract_answer,
    "SENTIMENT": sentiment,
    "SUMMARIZE": summarize,
    "TRANSLATE": translate,
}

selected_page = st.sidebar.selectbox("Select a page", page_names_to_funcs.keys())
page_names_to_funcs[selected_page]()