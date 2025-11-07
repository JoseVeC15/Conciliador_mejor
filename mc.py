import pandas as pd
import numpy as np
from datetime import datetime
import os
import re

import unicodedata
import difflib

# Intentar usar RapidFuzz para fuzzy matching (si está instalado). Si no, usamos difflib.
try:
    from rapidfuzz.fuzz import token_set_ratio, token_sort_ratio  # type: ignore
    HAVE_RAPIDFUZZ = True
except Exception:
    HAVE_RAPIDFUZZ = False

# --- CONTROL DE VERBOSIDAD: silenciar todos los prints cuando QUIET = True
import builtins

# Cambia a False si quieres ver nuevamente todos los mensajes impresos por el script
QUIET = False
if QUIET:
    def _no_print(*args, **kwargs):
        return None
    builtins.print = _no_print

class MatcheadorFacturas:
    def __init__(self):
        self.familias_df = None
        self.ventas_df = None
        self.banco_df = None
        self.resultados = None
    
    def cargar_datos(self, archivo_familias, archivo_ventas, archivo_banco):
        """Cargar los tres archivos Excel"""
        try:
            # 1. Cargar archivo de familias
            self.familias_df = pd.read_excel(archivo_familias)
            print(f"Familias cargadas: {len(self.familias_df)} familias")
            print(f"Columnas en familias: {list(self.familias_df.columns)}")
            
            # Verificar y ajustar columna Código si es necesario
            if 'Código' not in self.familias_df.columns:
                for col in self.familias_df.columns:
                    if 'codigo' in col.lower() or 'code' in col.lower():
                        self.familias_df.rename(columns={col: 'Código'}, inplace=True)
                        print(f"Columna '{col}' renombrada a 'Código'")
                        break
            
            # 2. Cargar ventas
            self.ventas_df = pd.read_excel(archivo_ventas)
            print(f"Ventas cargadas: {len(self.ventas_df)} registros")
            print(f"Columnas en ventas: {list(self.ventas_df.columns)}")
            
            # Verificar columnas en ventas
            self._verificar_columnas_ventas()
            
            # Extraer número de caja de la factura
            self._extraer_numero_caja()
            
            # Cargar extracto bancario con múltiples hojas
            self._cargar_extracto_banco(archivo_banco)
            
            # Normalizar nombres de columnas
            self._normalizar_columnas()
            
        except Exception as e:
            print(f"Error al cargar archivos: {e}")
    
    def _extraer_numero_caja(self):
        """Extraer el número de caja del número de factura"""
        print("\nExtrayendo número de caja de las facturas...")
        
        if 'nro_factura' not in self.ventas_df.columns:
            print("❌ No se encuentra la columna 'nro_factura' para extraer número de caja")
            return
        
        # Función para extraer el número de caja
        def extraer_caja(nro_factura):
            if pd.isna(nro_factura) or nro_factura == '':
                return ''
            
            nro_factura_str = str(nro_factura)
            
            # Patrón: números separados por guiones, el segundo grupo es la caja
            # Ejemplo: 001-"010"-0002238 -> "010" es la caja
            patron = r'(\d+)-"(\d+)"-(\d+)'
            coincidencia = re.match(patron, nro_factura_str)
            
            if coincidencia:
                numero_caja = coincidencia.group(2)  # El segundo grupo es la caja
                return numero_caja
            else:
                # Intentar otro patrón si el primero no funciona
                patron_alternativo = r'(\d+)-(\d+)-(\d+)'
                coincidencia_alt = re.match(patron_alternativo, nro_factura_str)
                if coincidencia_alt:
                    return coincidencia_alt.group(2)
                else:
                    # Si no coincide ningún patrón, devolver vacío
                    return ''
        
        # Aplicar la extracción a todas las facturas
        self.ventas_df['Numero_Caja'] = self.ventas_df['nro_factura'].apply(extraer_caja)
        
        # Estadísticas de extracción
        cajas_extraidas = self.ventas_df['Numero_Caja'].notna().sum()
        cajas_unicas = self.ventas_df['Numero_Caja'].nunique()
        
        print(f"✓ Números de caja extraídos: {cajas_extraidas} facturas")
        print(f"✓ Cajas únicas encontradas: {cajas_unicas}")
        
        # Mostrar distribución de cajas
        distribucion_cajas = self.ventas_df['Numero_Caja'].value_counts().head(10)
        print(f"Top 10 cajas más frecuentes:")
        for caja, count in distribucion_cajas.items():
            print(f"  - Caja {caja}: {count} facturas")
        
        # Mostrar ejemplos de extracción
        print(f"\nEjemplos de extracción:")
        ejemplos = self.ventas_df[['nro_factura', 'Numero_Caja']].head(5).values
        for nro_factura, caja in ejemplos:
            print(f"  '{nro_factura}' -> Caja: '{caja}'")
    
    def _verificar_columnas_ventas(self):
        """Verificar que existan las columnas específicas en ventas"""
        columnas_requeridas = ['mca_fecha', 'nro_factura', 'neto_gs', 'nombre']
        columnas_faltantes = [col for col in columnas_requeridas if col not in self.ventas_df.columns]
        
        if columnas_faltantes:
            print(f"ADVERTENCIA: Columnas faltantes en ventas: {columnas_faltantes}")
            print(f"Columnas disponibles: {list(self.ventas_df.columns)}")
        else:
            print("✓ Todas las columnas requeridas encontradas en ventas")
    
    def _cargar_extracto_banco(self, archivo_banco):
        """Cargar extracto bancario con múltiples hojas por fechas (formato dd.mm)"""
        try:
            # Leer todas las hojas del archivo
            xl = pd.ExcelFile(archivo_banco)
            bancos_list = []
            
            print(f"\nCargando hojas del extracto bancario: {xl.sheet_names}")
            
            for sheet_name in xl.sheet_names:
                print(f"Procesando hoja: {sheet_name}")
                df_temp = pd.read_excel(archivo_banco, sheet_name=sheet_name)
                
                # Mostrar columnas disponibles en esta hoja
                print(f"  - Columnas en hoja {sheet_name}: {list(df_temp.columns)}")
                print(f"  - Registros en hoja {sheet_name}: {len(df_temp)}")
                
                # Agregar información de origen
                df_temp['Hoja_Origen'] = sheet_name
                df_temp['Mes_Referencia'] = sheet_name
                
                bancos_list.append(df_temp)
            
            self.banco_df = pd.concat(bancos_list, ignore_index=True)
            print(f"\nOperaciones bancarias cargadas: {len(self.banco_df)} registros de {len(xl.sheet_names)} hojas")
            print(f"Columnas totales en banco: {list(self.banco_df.columns)}")
            
        except Exception as e:
            print(f"Error al cargar extracto bancario: {e}")
    
    def _normalizar_columnas(self):
        """Normalizar nombres de columnas para consistencia"""
        
        print("\n" + "="*50)
        print("NORMALIZACIÓN DE COLUMNAS")
        print("="*50)
        
        # RENOMBRAR COLUMNAS DE VENTAS según tus especificaciones
        mapeo_columnas_ventas = {
            'mca_fecha': 'Fecha',
            'nro_factura': 'Factura', 
            'neto_gs': 'Monto',
            'nombre': 'Nombre_Cliente'
        }
        
        # Aplicar renombrado solo para las columnas que existen
        mapeo_a_aplicar = {}
        for col_original, col_nuevo in mapeo_columnas_ventas.items():
            if col_original in self.ventas_df.columns:
                mapeo_a_aplicar[col_original] = col_nuevo
                print(f"✓ Ventas: '{col_original}' -> '{col_nuevo}'")
            else:
                print(f"✗ Ventas: Columna '{col_original}' no encontrada")
        
        self.ventas_df.rename(columns=mapeo_a_aplicar, inplace=True)
        
        # Si no se ha cargado el banco_df, detener aquí
        if self.banco_df is None:
            print("❌ ERROR: No se pudo cargar el archivo de banco")
            return False
        
        # NORMALIZAR COLUMNAS DEL BANCO - BÚSQUEDA FLEXIBLE
        print(f"\nColumnas disponibles en banco (antes de renombrar): {list(self.banco_df.columns)}")
        
        # Buscar columnas por patrones (case insensitive)
        columnas_encontradas = {}
        
        for col in self.banco_df.columns:
            col_lower = col.lower()
            
            # Buscar columna de NOMBRE
            if any(palabra in col_lower for palabra in ['nombre', 'nombr', 'cliente', 'titular', 'beneficiario']):
                if 'nombre' not in columnas_encontradas:
                    columnas_encontradas['Nombre'] = col
                    print(f"✓ Banco - Columna NOMBRE encontrada: '{col}'")
            
            # Buscar columna de FECHA
            elif any(palabra in col_lower for palabra in ['fecha', 'date', 'fec']):
                if 'Fecha' not in columnas_encontradas:
                    columnas_encontradas['Fecha'] = col
                    print(f"✓ Banco - Columna FECHA encontrada: '{col}'")
            
            # Buscar columna de MONTO (crédito)
            elif any(palabra in col_lower for palabra in ['crédito', 'credito', 'monto', 'importe', 'amount', 'valor']):
                if 'Monto' not in columnas_encontradas:
                    columnas_encontradas['Monto'] = col
                    print(f"✓ Banco - Columna MONTO encontrada: '{col}'")
            
            # Buscar otras columnas útiles
            elif any(palabra in col_lower for palabra in ['detalle', 'descrip']):
                if 'Detalle' not in columnas_encontradas:
                    columnas_encontradas['Detalle'] = col
                    print(f"✓ Banco - Columna DETALLE encontrada: '{col}'")
            
            elif 'comprobante' in col_lower:
                if 'Comprobante' not in columnas_encontradas:
                    columnas_encontradas['Comprobante'] = col
                    print(f"✓ Banco - Columna COMPROBANTE encontrada: '{col}'")
        
        # Aplicar renombrado
        mapeo_banco_a_aplicar = {v: k for k, v in columnas_encontradas.items()}
        print(f"\nRenombrando columnas del banco: {mapeo_banco_a_aplicar}")
        self.banco_df.rename(columns=mapeo_banco_a_aplicar, inplace=True)
        
        # Verificar que tenemos las columnas críticas
        columnas_criticas = ['Fecha', 'Nombre', 'Monto']
        columnas_faltantes = [col for col in columnas_criticas if col not in self.banco_df.columns]
        
        if columnas_faltantes:
            print(f"\n❌ ERROR: Columnas críticas faltantes en banco: {columnas_faltantes}")
            print(f"Columnas disponibles después del renombrado: {list(self.banco_df.columns)}")
            print("Por favor, verifica los nombres de columnas en tu archivo de banco.")
            return False
        
        print(f"\n✓ Columnas críticas verificadas: {columnas_criticas}")
        print(f"Columnas finales en banco: {list(self.banco_df.columns)}")
        
        # Asegurar que las fechas sean datetime (manejar formato dd.mm)
        print("\nPROCESANDO FECHAS...")
        
        # Procesar fechas de ventas
        if 'Fecha' in self.ventas_df.columns:
            self.ventas_df['Fecha'] = pd.to_datetime(self.ventas_df['Fecha'], errors='coerce')
            fechas_invalidas_ventas = self.ventas_df['Fecha'].isna().sum()
            if fechas_invalidas_ventas > 0:
                print(f"ADVERTENCIA: {fechas_invalidas_ventas} fechas inválidas en ventas")
            print(f"Fechas procesadas en ventas: {len(self.ventas_df) - fechas_invalidas_ventas} válidas")
        
        # Procesar fechas del banco
        if 'Fecha' in self.banco_df.columns:
            # Mostrar ejemplos de fechas antes de la conversión
            print(f"Ejemplos de fechas en banco (primeras 5): {self.banco_df['Fecha'].head().tolist()}")
            
            # Intentar diferentes formatos de fecha
            self.banco_df['Fecha'] = pd.to_datetime(self.banco_df['Fecha'], errors='coerce', dayfirst=True)
            
            fechas_invalidas_banco = self.banco_df['Fecha'].isna().sum()
            if fechas_invalidas_banco > 0:
                print(f"ADVERTENCIA: {fechas_invalidas_banco} fechas inválidas en banco")
                # Mostrar ejemplos de fechas problemáticas
                fechas_problematicas = self.banco_df[self.banco_df['Fecha'].isna()]['Fecha'].head(3)
                print(f"Ejemplos de fechas problemáticas: {fechas_problematicas.tolist()}")
            else:
                print("✓ Todas las fechas del banco convertidas correctamente")
            
            print(f"Fechas procesadas en banco: {len(self.banco_df) - fechas_invalidas_banco} válidas")
        
        # Filtrar solo operaciones con monto positivo (créditos) y que tengan nombre
        if 'Monto' in self.banco_df.columns and 'Nombre' in self.banco_df.columns:
            total_operaciones = len(self.banco_df)
            
            # Filtrar operaciones válidas
            mask = (self.banco_df['Monto'] > 0) & (self.banco_df['Nombre'].notna())
            self.banco_df = self.banco_df[mask]
            
            operaciones_filtradas = total_operaciones - len(self.banco_df)
            print(f"\nOperaciones filtradas (montos <= 0 o sin nombre): {operaciones_filtradas}")
            print(f"Operaciones válidas para matcheo: {len(self.banco_df)}")
        
        return True
    
    def _obtener_familia_por_persona(self, nombre_persona):
        """Buscar a qué familia pertenece una persona"""
        if pd.isna(nombre_persona) or nombre_persona == '':
            return 'No identificada'

        # Normalizar y tokenizar nombres para comparar sin importar el orden
        def _normalize_to_token_set(text):
            if pd.isna(text) or text == '':
                return frozenset()
            # Normalizar acentos y caracteres, dejar solo letras/dígitos y espacios
            s = str(text).upper().strip()
            s = unicodedata.normalize('NFKD', s)
            s = ''.join(ch for ch in s if not unicodedata.combining(ch))
            # Reemplazar caracteres no alfanuméricos por espacio
            s = re.sub(r'[^A-Z0-9\s]', ' ', s)
            tokens = [tok for tok in s.split() if tok]
            return frozenset(tokens)

        nombre_buscar_tokens = _normalize_to_token_set(nombre_persona)

        # Obtener todas las columnas que contienen personas
        columnas_persona = [col for col in self.familias_df.columns if 'persona' in col.lower()]
        columnas_persona.sort()  # Ordenar para procesar en orden

        for _, familia in self.familias_df.iterrows():
            # Revisar todas las columnas de personas
            for col in columnas_persona:
                if pd.notna(familia[col]):
                    valor_celda_tokens = _normalize_to_token_set(familia[col])
                    if not valor_celda_tokens:
                        continue
                    # Coincidencia si los tokens son exactamente iguales
                    # o si uno es subconjunto del otro (ej. 'JUAN PEREZ' vs 'PEREZ JUAN ANTONIO')
                    if (nombre_buscar_tokens == valor_celda_tokens or
                        nombre_buscar_tokens.issubset(valor_celda_tokens) or
                        valor_celda_tokens.issubset(nombre_buscar_tokens)):
                        return familia['Código']

        return 'No encontrada'
    

    def matcheo_exacto(self):
        """Realizar matcheo exacto por fecha, nombre y monto"""
        # Verificar que las columnas necesarias existen
        columnas_requeridas_banco = ['Fecha', 'Nombre', 'Monto']
        for col in columnas_requeridas_banco:
            if col not in self.banco_df.columns:
                print(f"❌ ERROR: No se puede realizar matcheo - Columna '{col}' no encontrada en banco")
                print(f"Columnas disponibles en banco: {list(self.banco_df.columns)}")
                return None
        
        columnas_requeridas_ventas = ['Fecha', 'Nombre_Cliente', 'Monto']
        for col in columnas_requeridas_ventas:
            if col not in self.ventas_df.columns:
                print(f"❌ ERROR: No se puede realizar matcheo - Columna '{col}' no encontrada en ventas")
                print(f"Columnas disponibles en ventas: {list(self.ventas_df.columns)}")
                return None
        
        resultados = []
        
        print("\n" + "="*50)
        print("INICIANDO MATCHEO EXACTO")
        print("="*50)
        print(f"Comparando {len(self.ventas_df)} ventas con {len(self.banco_df)} operaciones bancarias")
        
        # Contadores para seguimiento
        matches_encontrados = 0
        ventas_procesadas = 0
        
        for idx, venta in self.ventas_df.iterrows():
            if idx % 100 == 0:
                print(f"Procesando venta {idx + 1}/{len(self.ventas_df)}")
            
            # Validar que tengamos los datos necesarios
            if (pd.isna(venta.get('Fecha')) or 
                pd.isna(venta.get('Nombre_Cliente')) or 
                pd.isna(venta.get('Monto'))):
                continue
            
            ventas_procesadas += 1
            
            # Buscar coincidencia exacta en operaciones bancarias
            try:
                mask = (
                    (self.banco_df['Fecha'] == venta['Fecha']) &
                    (self.banco_df['Nombre'].str.upper() == str(venta['Nombre_Cliente']).upper()) &
                    (self.banco_df['Monto'] == venta['Monto'])
                )
                
                coincidencias = self.banco_df[mask]
                
                for _, banco in coincidencias.iterrows():
                    # Encontrar la familia correspondiente
                    familia = self._obtener_familia_por_persona(venta['Nombre_Cliente'])
                    
                    resultados.append({
                        'Detalle_Banco': banco.get('Detalle', ''),
                        'Familia': familia,
                        'Codigo_Familia': familia if familia != 'No encontrada' else '',
                        'Factura': venta.get('Factura', ''),
                        'Numero_Caja': venta.get('Numero_Caja', ''),
                        'Fecha_Factura': venta['Fecha'],
                        'Fecha_Banco': banco['Fecha'],
                        'Nombre_Banco': banco['Nombre'],
                        'Cliente_Factura': venta['Nombre_Cliente'],             
                        'Monto_Banco': banco['Monto'],
                        'Monto_Factura': venta['Monto'],
                        'Comprobante_Banco': banco.get('Comprobante', ''),
                        'Concepto_Banco': banco.get('Concepto', ''),
                        'Tipo_Match': 'EXACTO',
                        'Coincidencia': '100%',
                        'Estado': 'MATCH_EXACTO'
                    })
                    matches_encontrados += 1
                    
            except Exception as e:
                print(f"Error procesando venta {idx}: {e}")
                continue
        
        self.resultados = pd.DataFrame(resultados)
        print(f"\n" + "="*50)
        print(f"PROCESO COMPLETADO")
        print(f"="*50)
        print(f"- Ventas procesadas: {ventas_procesadas}")
        print(f"- Matches exactos encontrados: {matches_encontrados}")
        
        return self.resultados

    def matcheo_multifacturas_misma_familia_dia_caja(self):
        """Matcheo donde se agrupan facturas que cumplan TODAS estas condiciones:
        1. MISMA FAMILIA (mismo código de familia)
        2. MISMO DÍA (fecha exacta)
        3. MISMA CAJA (mismo número de caja)
        
        El proceso:
        1. Agrupa facturas que cumplan las 3 condiciones
        2. Suma los montos del grupo
        3. Busca operación bancaria que coincida en:
           - Monto total
           - Fecha exacta
           - Familia del pagante (debe ser la misma familia del grupo)

        Suposición: nos enfocamos en grupos con 2 o más facturas (agrupamientos que no fueron
        resueltos por el matcheo exacto).
        """
        # Verificar columnas necesarias
        columnas_requeridas_banco = ['Fecha', 'Nombre', 'Monto']
        columnas_requeridas_ventas = ['Fecha', 'Nombre_Cliente', 'Monto', 'Factura', 'Numero_Caja']

        for col in columnas_requeridas_banco:
            if col not in self.banco_df.columns:
                print(f"❌ ERROR: No se puede realizar matcheo multi-facturas - Columna '{col}' no encontrada en banco")
                return None

        for col in columnas_requeridas_ventas:
            if col not in self.ventas_df.columns:
                print(f"❌ ERROR: No se puede realizar matcheo multi-facturas - Columna '{col}' no encontrada en ventas")
                return None

        print("\n" + "="*50)
        print("INICIANDO MATCHEO MULTI-FACTURAS (MISMA FAMILIA + MISMO DÍA + MISMA CAJA)")
        print("="*50)
        print("Condiciones requeridas:")
        print("1. Facturas de la MISMA FAMILIA")
        print("2. Facturas del MISMO DÍA")
        print("3. Facturas de la MISMA CAJA")

        # Construir conjunto de operaciones bancarias ya matcheadas para evitar reuse
        matched_ops = set()
        if self.resultados is not None and len(self.resultados) > 0:
            for _, r in self.resultados.iterrows():
                try:
                    fecha_b = pd.to_datetime(r.get('Fecha_Banco'))
                except Exception:
                    fecha_b = r.get('Fecha_Banco')
                nombre_b = str(r.get('Nombre_Banco', '')).upper().strip()
                try:
                    monto_b = float(r.get('Monto_Banco'))
                except Exception:
                    monto_b = r.get('Monto_Banco')

                matched_ops.add((fecha_b, nombre_b, monto_b))

        # Identificar facturas ya matcheadas
        facturas_matcheadas = set()
        if self.resultados is not None and len(self.resultados) > 0:
            for val in self.resultados['Factura'].dropna().astype(str):
                # dividir por comas por si hay facturas múltiples
                facturas_matcheadas.update([x.strip() for x in val.split(',') if x.strip()])
        
        # Preparar ventas válidas, excluyendo las ya matcheadas
        ventas_validas = self.ventas_df[
            ~self.ventas_df['Factura'].astype(str).isin(facturas_matcheadas)
        ].dropna(subset=['Fecha', 'Nombre_Cliente', 'Monto']).copy()
        
        print(f"\nFacturas ya matcheadas previamente: {len(facturas_matcheadas)}")
        print(f"Facturas disponibles para nuevo matcheo: {len(ventas_validas)}")
        
        # Añadir código de familia
        ventas_validas['Codigo_Familia'] = ventas_validas['Nombre_Cliente'].apply(self._obtener_familia_por_persona)
        
        # Filtrar solo ventas con familia identificada
        ventas_validas = ventas_validas[~ventas_validas['Codigo_Familia'].isin(['No encontrada', 'No identificada'])]

        # Identificar operaciones bancarias ya matcheadas
        ops_matcheadas = set()
        if self.resultados is not None and len(self.resultados) > 0:
            for _, r in self.resultados.iterrows():
                try:
                    fecha_b = pd.to_datetime(r['Fecha_Banco'])
                except Exception:
                    fecha_b = r['Fecha_Banco']
                nombre_b = str(r['Nombre_Banco']).upper().strip()
                monto_b = float(r['Monto_Banco'])
                ops_matcheadas.add((fecha_b, nombre_b, monto_b))
        
        # Crear copia del banco_df excluyendo operaciones ya matcheadas
        banco_disponible = self.banco_df.copy()
        banco_disponible['_key'] = banco_disponible.apply(
            lambda x: (
                pd.to_datetime(x['Fecha']) if pd.api.types.is_datetime64_any_dtype(x['Fecha']) else x['Fecha'],
                str(x['Nombre']).upper().strip(),
                float(x['Monto'])
            ),
            axis=1
        )
        banco_disponible = banco_disponible[~banco_disponible['_key'].isin(ops_matcheadas)]
        
        print(f"Operaciones bancarias ya matcheadas: {len(ops_matcheadas)}")
        print(f"Operaciones bancarias disponibles: {len(banco_disponible)}")
        
        matches_nuevos = []
        matches_encontrados = 0

        # Agrupar por Fecha, Numero_Caja y Codigo_Familia (las tres condiciones requeridas)
        grupo_cols = ['Fecha', 'Numero_Caja', 'Codigo_Familia']
        grouped = ventas_validas.groupby(grupo_cols)
        total_grupos = len(grouped)
        print(f"\nProcesando {total_grupos} grupos de facturas...")
        
        grupos_procesados = 0
        for (fecha, numero_caja, codigo_familia), grupo in grouped:
            grupos_procesados += 1
            if grupos_procesados % 10 == 0:
                print(f"Procesado {grupos_procesados}/{total_grupos} grupos...")
            # Nos centramos en agrupaciones con 2 o más facturas
            if len(grupo) < 2:
                continue

            facturas = list(grupo['Factura'])
            montos = list(grupo['Monto'])
            total_monto = float(np.sum(montos))

            if grupos_procesados % 10 == 0:
                print(f"\nProcesando grupo: fecha={fecha}, caja={numero_caja}, familia={codigo_familia}")
                print(f"Facturas en grupo: {len(grupo)}, Monto total: {total_monto}")

            # Obtener la familia del grupo y verificar coincidencia en banco
            codigo_familia = grupo['Codigo_Familia'].iloc[0]  # todas las facturas del grupo son de la misma familia
            
            # Buscar operación bancaria que coincida con fecha y monto total, y que el pagador sea de la misma familia
            try:
                # Primero filtrar por fecha y monto (operaciones más rápidas)
                mask_fecha_monto = (banco_disponible['Fecha'] == fecha) & (banco_disponible['Monto'] == total_monto)
                candidatos_banco = banco_disponible[mask_fecha_monto]
                
                if len(candidatos_banco) > 0:
                    # Si hay candidatos, entonces verificar familia
                    mask_familia = candidatos_banco['Nombre'].apply(lambda x: self._obtener_familia_por_persona(x) == codigo_familia)
                    mask = mask_fecha_monto & banco_disponible.index.isin(candidatos_banco[mask_familia].index)
            except Exception:
                # en caso de problemas con tipos, intentar convertir fecha
                mask = (
                    (pd.to_datetime(banco_disponible['Fecha']) == pd.to_datetime(fecha)) &
                    (banco_disponible['Monto'] == total_monto) &
                    (banco_disponible['Nombre'].apply(lambda x: self._obtener_familia_por_persona(x) == codigo_familia))
                )

            coincidencias = banco_disponible[mask]

            for _, banco in coincidencias.iterrows():
                # Evitar usar operaciones ya matcheadas
                try:
                    fecha_b = pd.to_datetime(banco['Fecha'])
                except Exception:
                    fecha_b = banco['Fecha']
                nombre_b = str(banco['Nombre']).upper().strip()
                monto_b = float(banco['Monto'])

                if (fecha_b, nombre_b, monto_b) in matched_ops:
                    continue

                # Determinar familias del grupo (únicas) y presentar en una fila agrupada
                familias_set = set()
                for _, venta_row in grupo.iterrows():
                    fam = self._obtener_familia_por_persona(venta_row['Nombre_Cliente'])
                    if fam:
                        familias_set.add(fam)

                familia_val = ";".join(sorted(familias_set)) if len(familias_set) > 0 else 'No encontrada'
                codigo_familia_val = familia_val if familia_val != 'No encontrada' else ''

                # Crear una sola fila representando el grupo de facturas
                matches_nuevos.append({
                    'Detalle_Banco': banco.get('Detalle', ''),
                    'Familia': familia_val,
                    'Codigo_Familia': codigo_familia_val,
                    'Factura': ",".join([str(x) for x in facturas]),
                    'Numero_Caja': numero_caja,
                    'Fecha_Factura': fecha,
                    'Fecha_Banco': banco['Fecha'],
                    'Nombre_Banco': banco['Nombre'],
                    'Cliente_Factura': ";".join(sorted(grupo['Nombre_Cliente'].astype(str).str.upper().unique())),
                    'Monto_Banco': banco['Monto'],
                    'Monto_Factura': total_monto,
                    'Comprobante_Banco': banco.get('Comprobante', ''),
                    'Concepto_Banco': banco.get('Concepto', ''),
                    'Tipo_Match': 'MULTI_FACTURAS',
                    'Coincidencia': 'AGREGADO',
                    'Estado': 'MATCH_MULTI_FACTURAS',
                    'Cantidad_Facturas': len(facturas),
                    'Facturas_Agrupadas': ",".join([str(x) for x in facturas])
                })

                # Marcar esta operación bancaria como utilizada
                matched_ops.add((fecha_b, nombre_b, monto_b))
                matches_encontrados += 1

        # Concatenar nuevos matches a los resultados existentes
        if len(matches_nuevos) > 0:
            df_nuevos = pd.DataFrame(matches_nuevos)
            if self.resultados is None or len(self.resultados) == 0:
                self.resultados = df_nuevos
            else:
                self.resultados = pd.concat([self.resultados, df_nuevos], ignore_index=True)

        print(f"- Matches multi-facturas encontrados: {matches_encontrados}")
        return self.resultados

    def _validar_pertenencia_familia(self, nombre_persona):
        """Encuentra el código de familia al que pertenece una persona"""
        if pd.isna(nombre_persona) or nombre_persona == '':
            return None
        
        # Buscar en todas las columnas Persona_*
        for _, row in self.familias_df.iterrows():
            for col in [c for c in self.familias_df.columns if c.startswith('Persona_')]:
                if pd.notna(row[col]) and nombre_persona.strip().upper() in str(row[col]).strip().upper():
                    return row['Código']
        return None

    def matcheo_por_grupo_familiar(self):
        """Matcheo específico para casos donde el pagador y el cliente son de la misma familia"""
        print("\n" + "="*50)
        print("INICIANDO MATCHEO POR GRUPO FAMILIAR")
        print("="*50)

        if self.resultados is None:
            self.resultados = pd.DataFrame()

        # Obtener operaciones sin match previo
        matched_ops = set()
        if len(self.resultados) > 0:
            matched_ops = set(zip(
                self.resultados['Fecha_Banco'].astype(str),
                self.resultados['Nombre_Banco'].astype(str),
                self.resultados['Monto_Banco'].astype(float)
            ))

        matches_nuevos = []
        matches_encontrados = 0

        # Procesar cada operación bancaria no matcheada
        for _, op_banco in self.banco_df.iterrows():
            op_key = (str(op_banco['Fecha']), str(op_banco['Nombre']), float(op_banco['Monto']))
            if op_key in matched_ops:
                continue

            # Encontrar familia del pagador
            familia_pagador = self._validar_pertenencia_familia(op_banco['Nombre'])
            if not familia_pagador:
                continue

            # Buscar facturas de la misma familia
            for _, factura in self.ventas_df.iterrows():
                familia_cliente = self._validar_pertenencia_familia(factura['Nombre_Cliente'])
                
                if familia_cliente and familia_cliente == familia_pagador:
                    # Verificar monto y fecha
                    fecha_banco = pd.to_datetime(op_banco['Fecha'])
                    fecha_factura = pd.to_datetime(factura['Fecha'])
                    diff_dias = abs((fecha_banco - fecha_factura).days)
                    
                    if (abs(float(op_banco['Monto']) - float(factura['Monto'])) < 0.01 and 
                        diff_dias <= 1):  # Tolerancia de 1 día
                        
                        matches_encontrados += 1
                        matches_nuevos.append({
                            'Fecha_Banco': op_banco['Fecha'],
                            'Nombre_Banco': op_banco['Nombre'],
                            'Monto_Banco': op_banco['Monto'],
                            'Fecha_Factura': factura['Fecha'],
                            'Factura': factura['Factura'],
                            'Nombre_Cliente': factura['Nombre_Cliente'],
                            'Monto_Factura': factura['Monto'],
                            'Tipo_Match': 'Grupo Familiar',
                            'Grupo_Familiar': familia_pagador,
                            'Detalle_Match': f'Match por grupo familiar {familia_pagador}'
                        })
                        matched_ops.add(op_key)
                        break

        if matches_nuevos:
            nuevos_matches_df = pd.DataFrame(matches_nuevos)
            self.resultados = pd.concat([self.resultados, nuevos_matches_df], ignore_index=True)
            print(f"\nMatches por grupo familiar encontrados: {matches_encontrados}")
            print(f"Grupos familiares identificados: {len(set(match['Grupo_Familiar'] for match in matches_nuevos))}")

        return self.resultados

    def matcheo_multifacturas_misma_familia_mismo_dia_caja(self):
        """Matcheo inteligente por familia: para cada pago en el banco, busca combinaciones de
        2 o más facturas de la misma familia que SUMEN el monto del pago bancario.
        
        Proceso:
        1. Para cada operación bancaria:
           - Identifica la familia del pagador (usando familias_df)
           - Si no encuentra familia, intenta por apellido o nombre exacto
        2. Busca facturas de esa familia:
           - Prioriza facturas cercanas en fecha al pago
           - Considera grupos de 2 a 8 facturas
           - Suma los montos y compara con el pago (tolerancia ±0.01)
        3. Al encontrar match:
           - Marca las facturas como usadas
           - Registra el match con detalles de todas las facturas

        Ejemplo:
        Si el banco tiene un pago de $1000 de la familia "PEREZ":
        - Busca facturas de la familia PEREZ
        - Podría encontrar: factura1=$300, factura2=$400, factura3=$300
        - Total facturas = $1000 = monto del banco -> MATCH!
        
        Nota: Las facturas se ordenan por cercanía en fecha al pago bancario
        para favorecer agrupaciones temporalmente cercanas.
        """
        # Nuevo enfoque: iterar por operaciones bancarias, identificar la familia desde el nombre
        # del banco y luego buscar en ventas 2 o más facturas pertenecientes a esa familia cuya
        # suma de montos sea igual al monto de la operación bancaria.
        if self.familias_df is None:
            print("❌ ERROR: No hay datos de familias cargados para matcheo por familia")
            return None

        columnas_requeridas_banco = ['Fecha', 'Nombre', 'Monto']
        for col in columnas_requeridas_banco:
            if col not in self.banco_df.columns:
                print(f"❌ ERROR: No se puede realizar matcheo multi-familia - Columna '{col}' no encontrada en banco")
                return None

        # Preparar ventas válidas y columna con código de familia
        ventas_validas = self.ventas_df.dropna(subset=['Fecha', 'Nombre_Cliente', 'Monto']).copy()
        if 'Codigo_Familia_Venta' not in ventas_validas.columns:
            ventas_validas['Codigo_Familia_Venta'] = ventas_validas['Nombre_Cliente'].apply(self._obtener_familia_por_persona)

        # Flag para evitar reutilizar la misma factura en múltiples matches
        ventas_validas['_usado_en_match'] = False

        matches_nuevos = []
        matches_encontrados = 0

        # Conjunto de operaciones bancarias ya usadas
        matched_ops = set()
        if self.resultados is not None and len(self.resultados) > 0:
            for _, r in self.resultados.iterrows():
                try:
                    fecha_b = pd.to_datetime(r.get('Fecha_Banco'))
                except Exception:
                    fecha_b = r.get('Fecha_Banco')
                nombre_b = str(r.get('Nombre_Banco', '')).upper().strip()
                try:
                    monto_b = float(r.get('Monto_Banco'))
                except Exception:
                    monto_b = r.get('Monto_Banco')
                matched_ops.add((fecha_b, nombre_b, monto_b))

        # Función auxiliar para extraer apellido/clave
        def extraer_apellido(nombre):
            if pd.isna(nombre):
                return ''
            partes = str(nombre).upper().strip().split()
            return partes[-1] if len(partes) > 0 else ''

        # Tolerancia para comparación de montos
        TOL = 0.01

        # Iterar por cada operación bancaria
        for _, banco in self.banco_df.iterrows():
            try:
                fecha_b = pd.to_datetime(banco['Fecha'])
            except Exception:
                fecha_b = banco['Fecha']

            nombre_banco = str(banco['Nombre']).upper().strip() if pd.notna(banco['Nombre']) else ''
            monto_banco = float(banco['Monto']) if pd.notna(banco['Monto']) else 0.0

            if (fecha_b, nombre_banco, monto_banco) in matched_ops:
                continue

            # Identificar familia a partir del nombre en el banco
            codigo_familia_banco = self._obtener_familia_por_persona(banco['Nombre'])

            # Filtrar candidatos en ventas por familia
            if codigo_familia_banco not in ['No encontrada', 'No identificada', None, '']:
                # Buscar todas las facturas no usadas de la misma familia
                candidatos = ventas_validas[
                    (ventas_validas['Codigo_Familia_Venta'] == codigo_familia_banco) &
                    (~ventas_validas['_usado_en_match'])
                ].copy()
            else:
                # Si no se identificó familia, intentar usar apellido del nombre del banco
                clave = extraer_apellido(banco['Nombre'])
                if clave == '':
                    # Si no hay clave de familia, buscar por coincidencia exacta de nombre
                    candidatos = ventas_validas[
                        (ventas_validas['Nombre_Cliente'].str.upper().strip() == nombre_banco) &
                        (~ventas_validas['_usado_en_match'])
                    ].copy()
                else:
                    # Buscar por apellido en el nombre del cliente
                    candidatos = ventas_validas[
                        (ventas_validas['Nombre_Cliente'].str.upper().str.contains(clave)) &
                        (~ventas_validas['_usado_en_match'])
                    ].copy()

            # No hay suficientes candidatos -> continuar
            if len(candidatos) < 2:
                continue

            # Ordenar candidatos por fecha para priorizar facturas cercanas en el tiempo
            candidatos = candidatos.sort_values('Fecha')

            # Para evitar combinatoria excesiva, limitamos el tamaño del conjunto a considerar
            # Primero intentamos con facturas cercanas en fecha al registro del banco
            candidatos['diff_dias'] = abs((candidatos['Fecha'] - fecha_b).dt.days)
            
            # Ordenar por diferencia de días (priorizar cercanas) y luego por monto (descendente)
            candidatos = candidatos.sort_values(['diff_dias', 'Monto'], 
                                              ascending=[True, False]).reset_index(drop=True)
            
            # Limitar a los primeros N candidatos más cercanos en fecha para combinaciones
            MAX_CANDIDATOS = 15  # Aumentado para dar más flexibilidad a las combinaciones
            candidatos_restringidos = candidatos.head(MAX_CANDIDATOS)

            # Buscar combinaciones de 2..k facturas que sumen el monto del banco
            from itertools import combinations

            encontrados_para_banco = False
            max_k = min(8, len(candidatos_restringidos))  # Permitir hasta 8 facturas por grupo
            for k in range(2, max_k + 1):
                if encontrados_para_banco:
                    break
                for combo in combinations(range(len(candidatos_restringidos)), k):
                    idxs = list(combo)
                    subset = candidatos_restringidos.loc[idxs]
                    suma = float(subset['Monto'].sum())
                    
                    # Verificar si la suma de facturas es igual al monto del banco
                    if abs(suma - monto_banco) <= TOL:
                        # Registrar match
                        facturas = subset['Factura'].astype(str).tolist()
                        numero_cajas = subset['Numero_Caja'].astype(str).unique().tolist()
                        cantidad_facturas = len(facturas)

                        matches_nuevos.append({
                            'Detalle_Banco': banco.get('Detalle', ''),
                            'Familia': codigo_familia_banco,
                            'Codigo_Familia': codigo_familia_banco if codigo_familia_banco not in ['No encontrada', 'No identificada'] else '',
                            'Factura': ",".join(facturas),
                            'Numero_Caja': ",".join(numero_cajas),
                            'Fecha_Factura': ",".join([str(x) for x in subset['Fecha'].dt.strftime('%Y-%m-%d').unique()]) if pd.api.types.is_datetime64_any_dtype(subset['Fecha']) else ",".join([str(x) for x in subset['Fecha'].unique()]),
                            'Fecha_Banco': banco['Fecha'],
                            'Nombre_Banco': banco['Nombre'],
                            'Cliente_Factura': ";".join(sorted(subset['Nombre_Cliente'].astype(str).str.upper().unique())),
                            'Monto_Banco': banco['Monto'],
                            'Monto_Factura': suma,
                            'Comprobante_Banco': banco.get('Comprobante', ''),
                            'Concepto_Banco': banco.get('Concepto', ''),
                            'Tipo_Match': 'MULTI_FACTURAS_FAMILIA_BANCO_PRIMERO',
                            'Coincidencia': 'AGREGADO',
                            'Estado': 'MATCH_MULTI_FACTURAS_FAMILIA',
                            'Cantidad_Facturas': cantidad_facturas,
                            'Facturas_Agrupadas': ",".join(facturas)
                        })

                        # Marcar facturas como usadas y la operación bancaria como matcheada
                        ventas_validas.loc[subset.index, '_usado_en_match'] = True
                        matched_ops.add((fecha_b, nombre_banco, monto_banco))
                        matches_encontrados += 1
                        encontrados_para_banco = True
                        break

            # continuar con la siguiente operación bancaria

        # Agregar nuevos matches a resultados
        if len(matches_nuevos) > 0:
            df_nuevos = pd.DataFrame(matches_nuevos)
            if self.resultados is None or len(self.resultados) == 0:
                self.resultados = df_nuevos
            else:
                self.resultados = pd.concat([self.resultados, df_nuevos], ignore_index=True)

        print(f"- Matches multi-familia (banco->familia->ventas) encontrados: {matches_encontrados}")
        return self.resultados
    
    def generar_reporte_completo(self, archivo_salida):
        """Generar reporte Excel completo con múltiples hojas"""
        if self.resultados is None or len(self.resultados) == 0:
            print("No hay resultados para generar reporte")
            return
        
        try:
            with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
                # 1. Matches exactos
                self.resultados.to_excel(writer, sheet_name='Matches_Exactos', index=False)

                # Matches multi-facturas (si existen)
                if 'Tipo_Match' in self.resultados.columns and 'MULTI_FACTURAS' in self.resultados['Tipo_Match'].unique():
                    multi_df = self.resultados[self.resultados['Tipo_Match'] == 'MULTI_FACTURAS']
                    multi_df.to_excel(writer, sheet_name='Matches_MultiFacturas', index=False)
                # Matches multi-familia (si existen)
                if 'Tipo_Match' in self.resultados.columns and 'MULTI_FACTURAS_FAMILIA' in self.resultados['Tipo_Match'].unique():
                    fam_df = self.resultados[self.resultados['Tipo_Match'] == 'MULTI_FACTURAS_FAMILIA']
                    fam_df.to_excel(writer, sheet_name='Matches_MultiFacturas_Familia', index=False)
                
                # 2. Resumen por familia
                resumen_familias = self.resultados.groupby('Familia').agg({
                    'Factura': 'count',
                    'Monto_Factura': 'sum'
                }).rename(columns={'Factura': 'Cantidad_Matches', 'Monto_Factura': 'Total_Matcheado'})
                resumen_familias.to_excel(writer, sheet_name='Resumen_Familias')
                
                # 3. Resumen por caja
                resumen_cajas = self.resultados.groupby('Numero_Caja').agg({
                    'Factura': 'count',
                    'Monto_Factura': 'sum'
                }).rename(columns={'Factura': 'Cantidad_Matches', 'Monto_Factura': 'Total_Matcheado'})
                resumen_cajas.to_excel(writer, sheet_name='Resumen_Cajas')
                
                # 4. Facturas sin match
                # Tener en cuenta que para matches multi-facturas 'Factura' puede contener varias facturas separadas por comas
                facturas_con_match = set()
                if 'Factura' in self.resultados.columns:
                    for val in self.resultados['Factura'].dropna().astype(str).unique():
                        # dividir por comas y limpiar espacios
                        for f in [x.strip() for x in val.split(',') if x.strip() != '']:
                            facturas_con_match.add(f)

                facturas_sin_match = self.ventas_df[~self.ventas_df['Factura'].astype(str).isin(facturas_con_match)]
                facturas_sin_match.to_excel(writer, sheet_name='Facturas_Sin_Match', index=False)
                
                # 5. Operaciones bancarias sin match
                operaciones_con_match = self.resultados['Nombre_Banco'] + self.resultados['Fecha_Banco'].astype(str) + self.resultados['Monto_Banco'].astype(str)
                banco_sin_match = self.banco_df[
                    ~(self.banco_df['Nombre'] + self.banco_df['Fecha'].astype(str) + self.banco_df['Monto'].astype(str)).isin(operaciones_con_match)
                ]
                banco_sin_match.to_excel(writer, sheet_name='Operaciones_Sin_Match', index=False)
                
                # 6. Detalle de familias
                self.familias_df.to_excel(writer, sheet_name='Familias', index=False)
                
                # 7. Detalle del extracto bancario procesado
                self.banco_df.to_excel(writer, sheet_name='Extracto_Procesado', index=False)
                
                # 8. Ventas con cajas extraídas
                self.ventas_df.to_excel(writer, sheet_name='Ventas_Con_Cajas', index=False)
            
            print(f"✓ Reporte completo generado: {archivo_salida}")
            
        except Exception as e:
            print(f"Error al generar reporte: {e}")
    
    def estadisticas_detalladas(self):
        """Mostrar estadísticas detalladas del proceso"""
        if self.resultados is not None and len(self.resultados) > 0:
            total_facturas = len(self.ventas_df)
            facturas_matcheadas = len(self.resultados['Factura'].unique())
            total_operaciones = len(self.banco_df)
            operaciones_matcheadas = len(self.resultados)
            
            print(f"\n{'='*50}")
            print(f"ESTADÍSTICAS DETALLADAS")
            print(f"{'='*50}")
            print(f"Total facturas: {total_facturas}")
            print(f"Facturas con match: {facturas_matcheadas}")
            print(f"Facturas sin match: {total_facturas - facturas_matcheadas}")
            print(f"Tasa de match facturas: {(facturas_matcheadas/total_facturas)*100:.2f}%")
            print(f"Total operaciones bancarias: {total_operaciones}")
            print(f"Operaciones con match: {operaciones_matcheadas}")
            print(f"Tasa de match operaciones: {(operaciones_matcheadas/total_operaciones)*100:.2f}%")
            print(f"Total montos matcheados: ${self.resultados['Monto_Factura'].sum():,.2f}")
            
            # Estadísticas por familia
            print(f"\nTop 10 familias con más matches:")
            stats_familias = self.resultados['Familia'].value_counts().head(10)
            for familia, count in stats_familias.items():
                print(f"  {familia}: {count} matches")
            
            # Estadísticas por caja
            print(f"\nDistribución por caja:")
            stats_cajas = self.resultados['Numero_Caja'].value_counts().head(10)
            for caja, count in stats_cajas.items():
                print(f"  Caja {caja}: {count} matches")
                
        else:
            print("No hay resultados para mostrar estadísticas")

# Función de uso simplificado
def ejecutar_matcheo():
    """Función principal para ejecutar el matcheo"""
    matcheador = MatcheadorFacturas()
    
    # Configurar rutas de archivos
    archivos = {
        'familias': 'familias.xlsx',
        'ventas': 'ventas.xlsx',
        'banco': 'extracto_banco.xlsx'
    }
    
    # Verificar que los archivos existan
    for nombre, archivo in archivos.items():
        if not os.path.exists(archivo):
            print(f"❌ ERROR: No se encuentra el archivo {nombre}: {archivo}")
            return None
    
    # Cargar datos
    print("Cargando archivos...")
    try:
        matcheador.cargar_datos(
            archivo_familias=archivos['familias'],
            archivo_ventas=archivos['ventas'],
            archivo_banco=archivos['banco']
        )
        
        # Verificar que los DataFrames se cargaron correctamente
        if matcheador.familias_df is None:
            print("❌ ERROR: No se pudo cargar el archivo de familias")
            return None
        if matcheador.ventas_df is None:
            print("❌ ERROR: No se pudo cargar el archivo de ventas")
            return None
        if matcheador.banco_df is None:
            print("❌ ERROR: No se pudo cargar el archivo de banco")
            return None
        
        # Realizar matcheo exacto
        print("\nRealizando matcheo exacto...")
        resultados = matcheador.matcheo_exacto()
        
        if resultados is not None:
            # Intentar matcheo por multi-facturas (misma familia + mismo día + misma caja)
            print("\nRealizando matcheo multi-facturas (misma familia + mismo día + misma caja)...")
            matcheador.matcheo_multifacturas_misma_familia_dia_caja()
            resultados = matcheador.resultados
        
    except Exception as e:
        print(f"❌ ERROR durante el proceso: {str(e)}")
        return None
    
    # Generar reporte
    if resultados is not None and len(resultados) > 0:
        print("\nGenerando reporte...")
        matcheador.generar_reporte_completo('reporte_matches_completo.xlsx')
        
        # Mostrar estadísticas
        matcheador.estadisticas_detalladas()
    else:
        print("No se encontraron matches exactos")
    
    return matcheador

def main():
    """Función principal que inicia el proceso de matcheo"""
    try:
        print("Iniciando proceso de matcheo...")
        matcheador = ejecutar_matcheo()
        if matcheador is not None:
            print("Proceso completado exitosamente.")
            return 0
        else:
            print("El proceso no pudo completarse debido a errores.")
            return 1
    except Exception as e:
        print(f"Error durante la ejecución: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())
