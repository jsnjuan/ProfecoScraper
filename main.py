# -*- coding: utf-8 -*-
"""
Created on Fri Oct  2 01:24:07 2020

@author: jaime
"""

import time
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd 
from bs4 import BeautifulSoup
from operator import itemgetter
from datetime import datetime
import pickle

# Project directory
rt_cod = r''
# Chrome driver directory path
path_chrome_driver = r"\Drivers\chromedriver.exe"

# ========= START: UTILS ======================================================

def gen_basquet():
    df_basquet = pd.read_excel(rt_cod + r'\df_prods_L1.xlsx')
    df_basquet = df_basquet[ pd.notna(df_basquet['scrapper'])]
    return df_basquet[['producto', 'cve_prod']].apply(tuple, axis=1)

def gen_ls_muns():
    df_muns = pd.read_excel(rt_cod + r'\cves_ciudades_muns.xlsx', dtype=object)
    return df_muns

def gen_table_body(soup):
    h1 = [child for child in soup.children]
    h2 = [child for child in h1[0].children]
    h3 = [child for child in h2[2].children]
    h4 = [child for child in h3[1].children]
    h5 = [child for child in h4[1].children]
    h6 = [child for child in h5[13].children]
    table_body = h6[1]
    
    return table_body

def get_href(x):
    if x is not None:
        ls_as = x.find_all('a')
        if ls_as:
            a_tag = ls_as[0]
            if hasattr(a_tag, 'href'):
                a_href = a_tag['href']
                if a_href.startswith('lista'):
                    return a_href
    return None

def no_blank_text(x):
    if x is not None:
        text = x.get_text().strip()
        return text if text !='' else None
    return None
    
def gen_table_products(soup):
    table_body = gen_table_body(soup)
    
    data=[]
    rows = [r for r in table_body.find_all('tr') if hasattr(r, 'children')]
    for row in rows:
        cols = row.find_all('td')
        data.append([ele for ele in cols if ele])

    df = pd.DataFrame(data[1:])

    df1 = df.iloc[:,:-1].applymap(no_blank_text)
    df1 = df1.ffill()
        
    fn_lambda2 = lambda x: (no_blank_text(x), get_href(x))
    not_none = lambda t: None if t[0] == t[1] == None else t
    
    df2 = df.iloc[:,-1].map(lambda x: not_none(fn_lambda2(x)))
    
    df_prods = pd.concat([df1, df2], axis=1)
    df_prods.dropna(axis=1, how='all', inplace=True)    
    df_prods = df_prods.dropna(subset=[4])
    
    df_prods.columns = ['catalogo', 'categoria', 'subcategoria', 'producto']
    
    df_prods['enlace'] = df_prods['producto'].map(itemgetter(1)).str.strip()
    df_prods['producto'] = df_prods['producto'].map(itemgetter(0)).str.strip()

    df_prods['cve_prod'] = df_prods['enlace'].str.extract('codigo=(.*)&')
    
    return df_prods

def scrap_prod_marca(driver, prod):
    
    print(datetime.now())
    print('='*10, 'Marca:', prod)
    
    driver.execute_script("window.open();")

    driver.switch_to_window(driver.window_handles[2])

    driver.get(prod)
    
    tabla = driver.find_element(By.CLASS_NAME, "textos_tablas")
    
    encabezado = tuple(cell.text for cell in tabla.find_elements_by_tag_name('th')) 

    ls_filas = []
    st_enlaces_estab = set()
    
    driver.implicitly_wait(3)
    
    for row in tabla.find_elements_by_css_selector('tr'):
        tup = tuple(cell.text for cell in row.find_elements_by_tag_name('td'))
        
        enlace=None
        if row.find_elements_by_css_selector('a'):
            enlace = row.find_elements_by_css_selector('a')[0].get_attribute('href')
            st_enlaces_estab.add(enlace)
        
        ls_filas.append(tup + (enlace,))
    
    df = pd.DataFrame.from_records(ls_filas)
    
    df.columns = encabezado + ('cve_est',)
    
    df.dropna(how='all', inplace=True)

    driver.close()
    
    return st_enlaces_estab, df

def scrap_prod(driver, prod, enlace):
    def gen_prod_marca_pres(s):
        if len(s.split(',')) > 3:
            print('Existe más de tres tokens. Se vaciará todo en la cadena producto.')
            return s, '', ''
        elif len(s.split(',')) < 3:
            print('Existen menos de tres tokens. Se vaciará todo en la cadena producto.')
            return s, '', ''
        else:
            p, m, pr = (x.strip() for x in s.split(','))
            return p, m, pr
    
    print(datetime.now())
    print('='*10, 'Producto:', prod, 'enlace:', enlace)
    
    prefix_link = 'https://www.profeco.gob.mx/precios/canasta/'
    

    driver.execute_script("window.open();")
    
    driver.switch_to_window(driver.window_handles[1])

    driver.get(prefix_link + enlace)
    
    tabla = driver.find_element(By.CLASS_NAME, "textos_tablas")
        
    ls_prods = []
    ls_enlaces = []
    for row in tabla.find_elements_by_css_selector('tr'):
        URL = row.find_elements_by_css_selector('a')
        if URL:
            prod_marca_pres = URL[0].text
            liga = URL[0].get_attribute('href')
            print(liga)
            ls_prods.append((prod_marca_pres, liga))
            ls_enlaces.append(liga)
        else:
            ls_enlaces.append('')
    
    main_window = driver.current_window_handle

    st_estabs = set()
    ls_dfs_marcas = []
    for prod_marca_pres, prod in ls_prods:
        print(' - '*20)
        print('Mandando llamar prod_marca_presentación:', prod_marca_pres)
        st_estab, df_prod = scrap_prod_marca(driver, prod)
        df_prod['producto_marca_presentacion'] = prod_marca_pres
        prod, marca, pres = gen_prod_marca_pres(prod_marca_pres)
        df_prod['producto'] = prod
        df_prod['marca'] = marca
        df_prod['presentacion'] = pres
        df_prod['scrap_datetime'] = '{}'.format(datetime.now())
                
        time.sleep(30)
        st_estabs |= st_estab
        ls_dfs_marcas.append(df_prod)
        driver.switch_to_window(main_window)
        
    driver.close()
    
    return st_estabs, pd.concat(ls_dfs_marcas)
        
df_v = None
def scrap_basquet(s, basquet):
    global df_v
    
    mun, cve_mun, cve_ciudad, ciudad = (s['mun'], s['cve_mun'],
                                        s['cve_ciudad'], s['ciudad'])
    
    print(datetime.now())
    print('='*10, 'Ciudad:', ciudad, 'Municipio:', mun)
    
    driver=webdriver.Chrome(executable_path=path_chrome_driver)
    driver.implicitly_wait(9)    
    rt_qqp = r'https://www.profeco.gob.mx/precios/canasta/home.aspx?th=1'
    driver.get(rt_qqp)
    
    WebDriverWait(driver, 7).until(EC.frame_to_be_available_and_switch_to_it((By.ID,"ifrIzquierdo")))
    
    select_ciudad = Select(driver.find_element_by_id("cmbCiudad"))
    
    select_ciudad.select_by_value(cve_ciudad)
    
    select_mun = Select(driver.find_element_by_id("listaMunicipios"))
    
    select_mun.select_by_value(cve_mun)

    driver.find_element_by_id('ImageButton1').click()

    frame_arbol = 'ifrArbol'
    WebDriverWait(driver, 20).until(EC.frame_to_be_available_and_switch_to_it((By.ID,frame_arbol)))
    
    arbol_id = 'Arbol'
    try:
        driver.find_element_by_id(arbol_id).click()
    except:
        print('Operación interrumpida. No hay arbol en esta ciudad')
        return 

    html = driver.page_source
    soup = BeautifulSoup(html)

    df_prods = gen_table_products(soup)
    df_v = df_prods.copy(deep=True)
    
    filter_basquet = df_prods['cve_prod'].isin({y[1:] for _, y in basquet})
    df_prods = df_prods[filter_basquet]
    
    print('La tabla de productos se parseó bien')
    print(df_prods.head())
    
    prod_cve_prod_enlace = ['producto', 'cve_prod', 'enlace']
    tuplas_prod_enlace =  df_prods[prod_cve_prod_enlace].apply(tuple, axis=1)

    print('# de enlaces por abrir:', len(tuplas_prod_enlace))

    main_window = driver.current_window_handle
    
    ls_metadata = []

    ls_dfs_prods = []
    st_estabs = set()
    for prod, cve_prod, enlace in tuplas_prod_enlace:
        ini_scrap_prod = datetime.now()
        
        st_estab, df_prod = scrap_prod(driver, prod, enlace)
        st_estabs |= st_estab
        
        df_prod['producto'] = prod
        df_prod['cve_prod'] = cve_prod
        
        end_scrap_prod = datetime.now()
        
        ls_dfs_prods.append(df_prod)
        
        ls_metadata.append((mun, cve_mun, cve_ciudad, ciudad, 
                            prod, ini_scrap_prod, end_scrap_prod))
        
        driver.switch_to_window(main_window)
        # 60 segundos entre producto de la canasta
        time.sleep(60)
    
    df_total = pd.concat(ls_dfs_prods)
    
    driver.quit()
    return st_estabs, df_total, ls_metadata

# ========= END: UTILS ========================================================

basquet = gen_basquet()
ls_muns = gen_ls_muns()


ls_metadata_total = []

dc_muns = {}
for _, s in ls_muns.iterrows():
    
    st_estabs, df_total, ls_metadata_mun = scrap_basquet(s, basquet)
    ls_metadata_total.extend(ls_metadata_mun)
    
    clave_dc = s['mun'], s['cve_mun'],s['cve_ciudad'], s['ciudad']
    
    df_total['mun'] = s['mun']
    df_total['cve_mun'] = s['cve_mun']
    df_total['ciudad'] = s['ciudad']
    df_total['cve_ciudad'] = s['cve_ciudad']
    
    # Dumping records
    df_total.to_csv(r'{}_{}.csv'.format(clave_dc[1],clave_dc[2]))
    
    dc_muns[clave_dc] = st_estabs, df_total
    
    time.sleep(1_800)

df_metadata_total = pd.DataFrame(ls_metadata_total)

df_metadata_total.columns = ['mun', 'cve_mun', 'cve_ciudad', 'ciudad', 
                            'prod', 'ini_scrap_prod', 'end_scrap_prod']

