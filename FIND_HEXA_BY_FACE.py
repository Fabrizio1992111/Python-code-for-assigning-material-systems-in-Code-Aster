import time
import numpy as np
import subprocess
import os
import pandas as pd


mensaje = """
# -----------------------------------------------------------------------------

###########################################
# Corriendo: FIND_HEXA_BY_FACE.py         #
# Autor:     Santiago Rabazzi             #
# Año:       2024                         #
# @:         Instituto de Física Rosario  #
###########################################

SUBRUTINA

El script necesita como entradas dos dataframes estructurados:
    
1) df_hexa8: Todos los elementos HEXA con 8 nodos.
    
    Nodo1,Nodo2,Nodo3,Nodo4,Nodo5,Nodo6,Nodo7,Nodo8,M_ID
    
2) df_quad4_int: Todos los elementos face QUAD con 4 nodos.
    
    Nodo1,Nodo2,Nodo3,Nodo4,F_ID
    

El mismo va a buscar los elementos HEXA constituidos a partir de las FACE QUAD
dadas en df_quad4_int.

# -----------------------------------------------------------------------------
"""
print(mensaje)

cwd = os.getcwd().replace("\\", "/")

script_dir = cwd

df_hexa8 = pd.read_csv(f'{cwd}/df_hexa8.csv')
df_quad4 = pd.read_csv(f'{cwd}/df_quad4_int.csv') #SOLO CARA INTERIOR

df_quad4['F_ID'] = df_quad4['F_ID'].astype(str)

# Concatenar las cuatro columnas de nodos y obtener los valores únicos
lista_nodos_quad4 = pd.unique(df_quad4[['Nodo1', 'Nodo2', 'Nodo3', 'Nodo4']].values.ravel())

# Convertir la lista de nodos única en un conjunto para asegurar valores únicos
conjunto_nodos_quad4 = set(lista_nodos_quad4)

# Crear una nueva columna 'cantidad' en df_hexa8 e inicializarla con ceros
df_hexa8['cantidad'] = 0

mensaje = """
# -----------------------------------------------------------------------------
##                 LEYENDO DF8
# -----------------------------------------------------------------------------
"""
print(mensaje)

# Iterar sobre cada fila del DataFrame df_hexa8
for index, row in df_hexa8.iterrows():
    # Inicializar un contador de la cantidad de nodos presentes
    cantidad_nodos_presentes = 0
    
    # Verificar la presencia de cada nodo en las ocho columnas del elemento HEXA8
    for i in range(1, 9):
        # Obtener el nombre de la columna del nodo
        nodo_columna = f'Nodo{i}'
        
        # Verificar si el nodo está presente en conjunto_nodos_quad4
        if row[nodo_columna] in conjunto_nodos_quad4:
            # Si está presente, incrementar el contador
            cantidad_nodos_presentes += 1
    
    # Asignar el valor del contador a la columna 'cantidad' en la fila actual
    df_hexa8.at[index, 'cantidad'] = cantidad_nodos_presentes
    
df_hexa8_filtrado = df_hexa8[df_hexa8["cantidad"] == 4].reset_index()

#%%
mensaje = """
# -----------------------------------------------------------------------------
##                 INICIANDO BUSQUEDA
# -----------------------------------------------------------------------------
"""
print(mensaje)

# Registra el tiempo de inicio
inicio = time.time()

# df_hexa8_filtrado = df_hexa8_filtrado.head(1000)
# df_quad4 = df_quad4.head(1000)

def find_face(df_hexa8, df_quad4):
    
    df_quad4_copy = df_quad4.copy()
    
    # Crear una lista para almacenar los números de F_ID correspondientes a cada elemento HEXA8
    numeros_F_ID = []
    
    # Iterar sobre cada fila del DataFrame df_hexa8
    for index_hexa8, row_hexa8 in df_hexa8.iterrows():
        # print(index_hexa8)
        # print(len(df_quad4_copy))
        
        # Crear una lista para almacenar los números de F_ID encontrados para el elemento HEXA8 actual
        F_ID_encontrados = []
        filas_a_eliminar = []
        
        # Iterar sobre cada fila del DataFrame df_quad4
        for index_quad4, row_quad4 in df_quad4_copy.iterrows():
            
            count = 0
            
            # Verificar si algún nodo del elemento HEXA8 actual está presente en la fila del DataFrame df_quad4
            for i in range(1, 9):
                
                nodo = row_hexa8[f'Nodo{i}']
                
                if nodo in [row_quad4['Nodo1']]:
                    
                    count = count+1
                
                if nodo in [row_quad4['Nodo2']]:
                    
                    count = count+1
                    
                if nodo in [row_quad4['Nodo3']]:
                    
                    count = count+1
                    
                if nodo in [row_quad4['Nodo4']]:
                    
                    count = count+1
                    
                if count == 4:
                    
                    F_ID_encontrados.append(row_quad4['F_ID'])
                    
                    # Borro esa linea
                    filas_a_eliminar.append(index_quad4)
                    df_quad4_copy = df_quad4_copy.drop(filas_a_eliminar) 
                    break
                    # print(df_quad4_copy)
    
        # Agregar la lista de F_ID encontrados para el elemento HEXA8 actual a la lista principal
        numeros_F_ID.append(F_ID_encontrados)
    
    # Agregar la lista de números de F_ID como una nueva columna en el DataFrame df_hexa8
    df_hexa8['F_ID'] = numeros_F_ID
    
    return df_hexa8

df_hexa8_final = find_face(df_hexa8_filtrado, df_quad4)

# Registra el tiempo de finalización
fin = time.time()

# Calcula la diferencia de tiempo
tiempo_transcurrido = fin - inicio

print(f"La busqueda tardó {tiempo_transcurrido} segundos en ejecutarse.")

#%%

def explode_df(df, col):
    exploded = df[col].apply(pd.Series).stack().reset_index(level=1, drop=True).rename(col)
    df_exploded = pd.concat([df.drop(col, axis=1), exploded], axis=1)
    return df_exploded

def verify_concat(df_hexa8, df_quad4):
    
    merged_df = None
    
    # Verificar los tamaños antes de concatenar
    if len(df_hexa8) == len(df_quad4):
        
        df_hexa8_str = explode_df(df_hexa8, 'F_ID')
        merged_df = pd.merge(df_hexa8_str[['M_ID', 'F_ID']], df_quad4[['F_ID']], on='F_ID', how='left')
        
        print("                                   ")
        print("----------HEXA8_FILTRADO-----------")
        print(merged_df.head())
    else:
        print("Error: Tamaños de [df_hexa8] y [df_quad4] no coinciden")
        print("----------")
        
    return merged_df

df_hexa8_output = verify_concat(df_hexa8_final, df_quad4)


df_hexa8_layer1 = df_hexa8_output.copy()

# df_hexa8_layer1['M_ID'] = df_hexa8_layer1['M_ID'] + 1 #PARA PASARSELO A CODE ASTER, LA NUMERACION ARRANCA EN 1
# df_hexa8_layer1['M_ID'] = 'M' + df_hexa8_layer1['M_ID'].astype(str)
# print(df_hexa8_layer1.head())

df_hexa8_layer1.to_csv(f'{script_dir}/df_hexa8_layer_interior.csv', index=False)

mensaje = """
# -----------------------------------------------------------------------------
    END SCRIPT
# -----------------------------------------------------------------------------
"""
print(mensaje)

