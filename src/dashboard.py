import sqlite3
import time
import pandas as pd
import streamlit as st

DB = '/home/vboxuser/resultados/dashboard.db'

st.set_page_config(page_title='Accidentes en tiempo real', layout='wide')
st.title('Accidentes de tránsito en tiempo real')


def leer(tabla):
    try:
        con = sqlite3.connect(DB, timeout=30)
        df = pd.read_sql_query('SELECT * FROM ' + tabla, con)
        con.close()
        return df
    except Exception:
        return pd.DataFrame()


horario = leer('horario')
zonas = leer('zonas')
clima = leer('clima')
ritmo = leer('ritmo')
alertas = leer('alertas')

if horario.empty:
    st.info('Esperando datos... verifique que el productor y el consumidor estén corriendo.')
else:
    c1, c2, c3 = st.columns(3)
    c1.metric('Accidentes recibidos', int(horario['accidentes_en_vivo'].sum()))
    c2.metric('Víctimas', int(horario['victimas'].sum()))
    c3.metric('Alertas graves', len(alertas))

    izq, der = st.columns(2)
    with izq:
        st.subheader('Accidentes por hora del día')
        st.bar_chart(horario.sort_values('hora_dia').set_index('hora_dia')['accidentes_en_vivo'])
        st.subheader('Ritmo de llegada (por minuto)')
        if not ritmo.empty:
            st.line_chart(ritmo.sort_values('minuto').set_index('minuto')['accidentes'])
    with der:
        st.subheader('Zonas con más accidentes')
        if not zonas.empty:
            st.map(zonas[['lat', 'lon']])
            st.dataframe(zonas[['zona', 'accidentes', 'victimas']].head(10), hide_index=True)
        st.subheader('Condiciones meteorológicas')
        if not clima.empty:
            st.bar_chart(clima.set_index('clima')['accidentes'])

    st.subheader('Últimas alertas graves')
    if not alertas.empty:
        st.dataframe(alertas.sort_values('event_time', ascending=False).head(10),
                     hide_index=True, use_container_width=True)

time.sleep(5)
st.rerun()
