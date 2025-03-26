import math as m
import numpy as np
import pandas as pd
import os
from code_aster.MacroCommands.Utils import partition 

mm = partition.MAIL_PY()  #Convierte la malla a formato py


def rot_vec_v2(script_dir, MESH, GROUP_Q4, GROUP_Q4_INT, GROUP_HEX8, FIND_HEXA_BY_FACE):
   
   # SELECCIONAR MALLA
    mm.FromAster(MESH)

    
    ##----------------------------------------------------------------------------------------
    ## COORDENADAS NODOS
    ##----------------------------------------------------------------------------------------
    
    coordenadas = mm.cn
    n_coor = pd.DataFrame(coordenadas, columns=['X', 'Y', 'Z'])
    n_coor ['ID'] = [str(i) for i in range(len(n_coor))]
    
    print("                          ")
    print("----------COORD-----------")
    print(n_coor.head())
    
    
    ##----------------------------------------------------------------------------------------
    ## QUAD4 - VERIFICACION
    ##----------------------------------------------------------------------------------------
    
    # La tabla de conectividad
    connex = mm.co
    
    # Filtrar solo los arrays con 4 componentes (elementos QUAD4)
    quadelem = [elemento for elemento in connex if len(elemento) == 4]
    quadelem = pd.DataFrame(quadelem, columns=['Nodo1', 'Nodo2', 'Nodo3', 'Nodo4'])
    
    print(quadelem)
    
    # Extraer ID de todas las Faces
    some_condition = True
    quadelem_id = mm.gma.get("MESH_QUAD4" if some_condition else "FACE")
   # quadelem_id = mm.gma.get("MESH_QUAD4")
    quadelem_id = pd.DataFrame(quadelem_id, columns=['F_ID']) 
    
    # Extraer ID de todas las SubFaces
    quadelem2_id = mm.gma.get(GROUP_Q4)
    quadelem2_id = pd.DataFrame(quadelem_id, columns=['F_ID'])
    
    print(quadelem_id)
    
    # Verificar los tamaños antes de concatenar
    if len(quadelem) == len(quadelem_id):
        
        quad_df = pd.concat([quadelem, quadelem_id], axis=1)
        
        # FILTRO SOLO LOS ELEMENTOS EN EL CONJUNTO DADO
        quad_df = quad_df.merge(quadelem2_id, on='F_ID')
        
        print("                          ")
        print("----------QUAD4-----------")
        print(quad_df.head())
        
        quad_df.to_csv(f'{script_dir}/df_quad4.csv', index=False)
    
    
    else:
        print("Error: Tamaños de [quadelem] y [quadelem_id] no coinciden")
        print("Revisar: GROUP_Q4")
        
        print("CANTIDAD ELEM QUAD4 EN MALLA = ", len(quadelem))
        print("CANTIDAD ELEM QUAD4 EN GRUPO: ", GROUP_Q4, " = ", len(quadelem_id)) 
    
        
    ##----------------------------------------------------------------------------------------
    ## QUAD4_INT - VERIFICIACION
    ##----------------------------------------------------------------------------------------    
    
    # Extraer ID de todas las Faces INT
    quadelem_int_id = mm.gma.get(GROUP_Q4_INT)
    quadelem_int_id = pd.DataFrame(quadelem_int_id, columns=['F_ID'])
    
    # Filtrar las conectividades
    quad_df_int = quad_df.loc[quad_df['F_ID'].isin(quadelem_int_id['F_ID'])]
    
    print("                              ")
    print("----------QUAD4_INT-----------")
    print(quad_df_int.head())
    
    quad_df_int.to_csv(f'{script_dir}/df_quad4_int.csv', index=False)
    
    ##----------------------------------------------------------------------------------------
    ## HEXA8 - VERIFICACION
    ##----------------------------------------------------------------------------------------
    
    # Filtrar solo los arrays con 8 nodos
    hexelem = [elemento for elemento in connex if len(elemento) == 8]
    hexelem = pd.DataFrame(hexelem, columns=['Nodo1', 'Nodo2', 'Nodo3', 'Nodo4', 'Nodo5', 'Nodo6', 'Nodo7', 'Nodo8'])
    
    # Extraer ID de TODOS
    #hexelem_id = mm.gma.get('MESH_HEXA8')
    some_condition = True
    hexelem_id = mm.gma.get("MESH_HEXA8" if some_condition else "MESH_HEXA88")
    hexelem_id = pd.DataFrame(hexelem_id, columns=['M_ID'])    
    
    # Extraer ID de SUBGRUPO
    hexelem2_id = mm.gma.get(GROUP_HEX8)
    hexelem2_id = pd.DataFrame(hexelem_id, columns=['M_ID'])
    
    # Verificar los tamaños antes de concatenar
    if len(hexelem) == len(hexelem_id):
    
        hexa_df = pd.concat([hexelem, hexelem_id], axis=1)
        
        # FILTRO SOLO LOS ELEMENTOS EN EL CONJUNTO DADO
        hexa_df = hexa_df.merge(hexelem2_id, on='M_ID')
        
        print("                          ")
        print("----------HEXA8-----------")
        print(hexa_df.head())
    else:
        print("Error: Tamaños de [hexelem] y [hexelem_id] no coinciden")
        print("Revisar: GROUP_HEXA8")
        
        print("CANTIDAD ELEM HEXA8 EN MALLA = ", len(hexelem))
        print("CANTIDAD ELEM HEXA8 EN GRUPO: ", GROUP_HEX8, " = ", len(hexelem_id)) 
        
    hexa_df.to_csv(f'{script_dir}/df_hexa8.csv', index=False)
    
    
    ##----------------------------------------------------------------------------------------
    ## ELEMENTOS HEXAS COINCIDENTES CON FACE_INT
    ##----------------------------------------------------------------------------------------
    ## ESTO HAY QUE HACERLO UNA SOLA VEZ. CONSUME MUCHO TIEMPO.
    ## CORRIENDO POR SPYDER TARDA MENOS.
    
    if FIND_HEXA_BY_FACE == True:
        
        import subprocess
        import os
        
        cwd = os.getcwd().replace("\\", "/")
        
        newdir = f'{script_dir}'
        scripts =  ["FIND_HEXA_BY_FACE.py"]
        
        for script in scripts:
            
            os.chdir(newdir)
            print(f"Directorio cambio a: {newdir}")
            
            # Ejecuta el script y captura la salida
            proceso = subprocess.Popen(["python", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            salida, error = proceso.communicate()
        
            # Imprime la salida del script
            if salida:
                print(salida.decode().rstrip())  # Convertir bytes a cadena y eliminar caracteres de nueva línea
            if error:
                print(f"Error en el script {script}:")
                print(error.decode().rstrip())  # Convertir bytes a cadena y eliminar caracteres de nueva línea
        
            # Espera a que termine el script
            proceso.wait()
            print(f"Script {script} finalizado.")
            print(" ")
            
            # Volver al directorio actual
            os.chdir(cwd)
            
        df_hexa8_layer_interior = pd.read_csv(f'{script_dir}/df_hexa8_layer_interior.csv')
        
    else:
        
        df_hexa8_layer_interior = pd.read_csv(f'{script_dir}/df_hexa8_layer_interior.csv')
        
        print("FIND_HEXA_BY_FACE NO ACTIVADO")
        
    ##----------------------------------------------------------------------------------------
    ## CALCULO DE VECTOR NORMAL - SOLO EN quad_df_int
    ##----------------------------------------------------------------------------------------    
    
    def calcular_vector_normal(df_nodos, df_conectividades):
    
        """
        Calcula el vector normal a una cara QUAD4 dada su conectividad y las coordenadas de los nodos.
        
        Args:
            df_nodos (pandas.DataFrame): DataFrame con las coordenadas xyz de los nodos, con columnas 'X', 'Y', 'Z', e 'ID'.
            df_conectividades (pandas.DataFrame): DataFrame con las conectividades de las caras QUAD4, con columnas 'Nodo1', 'Nodo2', 'Nodo3', 'Nodo4', y 'F_ID'.
    
        Returns:
            pandas.DataFrame: DataFrame con las caras QUAD4 y sus vectores normales normalizados.
        """
        resultados = []
    
        for index, row in df_conectividades.iterrows():
            # Extraer las coordenadas de los nodos que forman la cara
            nodos_cara = row[['Nodo1', 'Nodo2', 'Nodo3', 'Nodo4']]
            coordenadas_cara = df_nodos.loc[nodos_cara, ['X', 'Y', 'Z']].values
            
            # Calcular los vectores que definen los lados de la cara
            lados = np.diff(np.vstack((coordenadas_cara, coordenadas_cara[0])), axis=0)
            
            # Calcular el producto cruz entre dos vectores adyacentes para obtener el vector normal
            vector_normal = np.cross(lados[1], lados[0])
            
            # Normalizar el vector normal
            magnitud = np.linalg.norm(vector_normal)
            if magnitud != 0:
                vector_normal = vector_normal / magnitud
            
            vector_normal = np.round(vector_normal, 6)
            resultados.append(vector_normal)
    
        # Agregar los resultados al DataFrame de conectividades
        df_resultado = df_conectividades.copy()
        df_resultado['Vector_Normal'] = resultados
    
        return df_resultado
    
    # Calcular los vectores normales para las caras QUAD4
    df_res = calcular_vector_normal(n_coor, quad_df_int)
    
    print("                                ")
    print("----------VECT_NORMAL-----------")
    print(df_res.head())
    
    ##----------------------------------------------------------------------------------------
    ## ANGULOS
    ##----------------------------------------------------------------------------------------    
    
    # Función para calcular los ángulos náuticos a partir del vector normal
    def calcular_angulos_nauticos(vector_normal):
        
        x, y, z = vector_normal
        r = np.sqrt(x**2 + y**2 + z**2)
        
        Betta = np.arcsin(z / r)    # Inclinación (Betta)
        Alpha = np.arctan2(y, x)    # Azimut (Alpha)
        
        Alpha = Alpha * 180 / m.pi
        Alpha = round(Alpha, 2)
        
        Betta = Betta * 180 / m.pi
        Betta = round(Betta, 2)
        
        return Betta, Alpha
    
    # Aplicar la función a cada fila del DataFrame y asignar los resultados a nuevas columnas
    df_res[['Betta', 'Alpha']] = df_res['Vector_Normal'].apply(lambda x: pd.Series(calcular_angulos_nauticos(x)))
    df_res.drop(columns=['Vector_Normal'], inplace=True)
    
    df_quad4_int_angle = df_res.copy()
    
    print("                            ")
    print("----------ANGULOS-----------")
    print(df_quad4_int_angle.head())
    
    #df_quad4_int_angle.to_csv(f'{script_dir}df_quad4_int_angle.csv', index=False)
    
    ##----------------------------------------------------------------------------------------
    ## MERGE DATAFRAMES: DF_HEXA8_LAYER_INTERIOR + DF_QUAD4_CON_ANGULOS
    ##----------------------------------------------------------------------------------------  
    
    def explode_df(df, col):
        exploded = df[col].apply(pd.Series).stack().reset_index(level=1, drop=True).rename(col)
        df_exploded = pd.concat([df.drop(col, axis=1), exploded], axis=1)
        return df_exploded
    
    def verify_concat(df_hexa8, df_quad4):
        
        merged_df = None
        
        # Verificar los tamaños antes de concatenar
        if len(df_hexa8) == len(df_quad4):
            
            df_hexa8_str = explode_df(df_hexa8, 'F_ID')
            merged_df = pd.merge(df_hexa8_str[['M_ID', 'F_ID']], df_quad4[['F_ID', "Alpha", "Betta"]], on='F_ID', how='left')
            
            print("                                   ")
            print("----------HEXA8_FILTRADO-----------")
            print(merged_df.head())
        else:
            print("Error: Tamaños de [df_hexa8] y [df_quad4] no coinciden")
            print("----------")
            
        return merged_df
    
    df_hexa8_layer_int_angle = verify_concat(df_hexa8_layer_interior, df_quad4_int_angle)
    
    ##----------------------------------------------------------------------------------------
    ## COMPLETAR VOLUMENES FALTANTES
    ##----------------------------------------------------------------------------------------  
    ## !!!!!!! VALIDO SOLO PARA MALLAS ESTRUCTURADAS !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    def complete_datafame(df_hexa8, df_hexa8_incomplete): #incomplete df_hexa8_layer_interior
        
        # Creamos un DataFrame con una lista que va desde el menor M_ID hasta len(df_hexa8)
        menor_M_ID = df_hexa8['M_ID'].min()
        mayor_M_ID = df_hexa8['M_ID'].max()
        lista_M_ID = np.arange(menor_M_ID, mayor_M_ID + 1, 1)
        df_M_ID = pd.DataFrame({'M_ID': lista_M_ID})
    
        # Fusionamos los dataframes df_hexa8 y df_M_ID
        df_fusionado = pd.merge(df_M_ID, df_hexa8_incomplete, on='M_ID', how='left')
    
        # Ordeno de mayor a menor antes del FILL
        df_fusionado.sort_values(by='M_ID', ascending=False, inplace=True)
    
        # Iteramos sobre las columnas Alpha y Betta y rellenamos los valores NaN con el primer valor no nulo que encuentre
        for col in ['Alpha', 'Betta']:
            df_fusionado[col].fillna(method='ffill', inplace=True)
    
        df_fusionado['F_ID'].fillna(0, inplace=True)
    
        # Ordeno de mayor a menor antes del FILL
        df_fusionado.sort_values(by='M_ID', ascending=True, inplace=True)
    
        print("                                            ")
        print("----------HEXA8_FILTRADO_COMPLETO-----------")
        #print(df_fusionado.head())
    
        df_fusionado['M_ID'] = df_fusionado['M_ID'] + 1 #PARA PASARSELO A CODE ASTER, LA NUMERACION ARRANCA EN 1
        df_fusionado['M_ID'] = 'M' + df_fusionado['M_ID'].astype(str)
        print(df_fusionado.head())
        
        return df_fusionado
    
    df_hexa8_full_angle = complete_datafame(hexa_df, df_hexa8_layer_int_angle)
    
    return df_hexa8_full_angle
