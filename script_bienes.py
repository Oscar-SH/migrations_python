import pandas as pd
import mysql.connector
from datetime import datetime
from hermetrics.hamming import Hamming

def get_connection_mysql(ip: str, db: str, usuario: str, passw: str):
    cnxn = mysql.connector.connect(host=ip, database=db, user=usuario, password=passw)
    print(f'Connecting to {ip} in {db}')
    return cnxn


def get_query(query: str):
    result = get_connection_mysql('10.30.13.6', 'db_sica', 'root', 'siscae1035')
    cursor = result.cursor(buffered=True)
    cursor.execute(query)
    res = cursor.fetchall()
    result.close()
    return res


def get_dic_excel(nombre: str, hoja: str):
    data = pd.read_excel('./'+nombre, sheet_name=hoja)
    records = data.to_dict(orient='records')
    print(f'Reading excel {nombre}')
    return records


def format_fecha(fecha):
    fecha_n = 'NULL'
    hoy = datetime.now()
    if type(fecha) == str:
        try:
            fecha = datetime.strptime(fecha, '%d/%m/%Y')
        except ValueError:
            fecha = datetime.strptime('2023-06-29', '%Y-%m-%d')
    else:
        if str(fecha) == 'NaT' or str(fecha) == 'nan' or str(fecha) == 'None':
            return fecha_n
    fecha_n = str(fecha.strftime('%Y-%m-%d %H:%M:%S.%f'))
    ahora = str(hoy.strftime('%Y-%m-%d %H:%M:%S.%f'))
    if fecha_n > ahora:
        fecha_final = f"'2023-06-29'"
    else:
        fecha_final = f"'{fecha_n}'"
    return fecha_final


def format_string_to_null(string: str):
    if (string == 'nan'):
        cadena = 'NULL'
    else:
        cadena = str(string).replace("'", "")
        cadena = f"'{cadena}'"
    return cadena


def format_number_to_status(entero):
    status = 'ACTIVO' if entero == 0 else 'BAJA'
    if entero == 0:
        status = "'ACTIVO'"
    elif entero == 1:
        status = "'BAJA'"
    elif entero == 'FALSO':
        status = "'ACTIVO'"
    elif entero == 'VERDADERO':
        status = "'BAJA'"
    return status


def format_number_to_decimal(entero: float):
    costo = "{:.2f}".format(entero)
    return costo


def format_clave_cae(string: str, consecutivo: str):
    if len(consecutivo) == 1:
        numero = '00'+consecutivo
    elif len(consecutivo) == 2:
        numero = '0'+consecutivo
    else:
        numero = consecutivo
    clave = string[0:4] + '-' + string[4:7] + '-' + numero
    return numero if len(string) == 0 else clave


def is_in_catalog(text: str, catalogo, pb, pr):
    ham = Hamming()
    lista_nombres = []
    for x in catalogo:
        if text == x[pb]:
            return x[pr]
        lista_nombres.append(x[pb])

    result = max(lista_nombres, key=lambda cadena:ham.similarity(cadena, text))

    for arr in catalogo:
        if result == arr[pb]:
            return arr[pr]


def convert_mueble_id(bienes):
    lista = []
    proveedores = get_dic_excel('Catálogo de Proveedores.xlsx', 'Catálogo de Proveedores')
    inv_mueble = get_query("SELECT * FROM inv_cat_mueble")
    cat_ubicaciones = get_query("SELECT * FROM cat_ubicaciones WHERE id_ubicacion")
    for row in bienes:
        for mueble in inv_mueble:
            if str(row['BMI_CTA_CON']) == mueble[1]:
                row['ID_TIPO_MUEBLE'] = mueble[0]
                nombre = mueble[2].replace("'", "")
                row['DES_NOMBRE'] = f"'{nombre}'"
            else:
                pass
        for prov in proveedores:
            if row['BMI_PRO'] == prov['PRO_CVE']:
                provedor = str(prov['PRO_DES']).replace("'", "")
                row['BMI_PRO'] = f"'{provedor}'"
        row['BMI_AREA'] = is_in_catalog(row['DESCRIPCIO'], cat_ubicaciones, 1, 0)
        lista.append(row)
    return lista


def convert_catalogos_inventarios(bienes):
    lista = []
    inventarios = get_dic_excel("Inventario.xlsx", "Inventario")
    cat_programas = get_query("SELECT * FROM cat_programas")
    inv_valuaciones = get_query("SELECT * FROM inv_valuaciones")
    inv_adquisiciones = get_query("SELECT * FROM inv_adquisiciones")
    inv_especialidades = get_query("SELECT * FROM inv_cat_especialidad")
    for row in bienes:
        for inv in inventarios:
            if str(row['CLAVE_CAE']) == "'" + str(inv['txtCtaCon']) + "'":
                adqui = inv['Tipo de Adquisición']
                val = inv['Cuadro combinado418']
                program = inv['Cuadro combinado426']
                espe = inv['Especialidad ']
        for adquisicion in inv_adquisiciones:
            if adqui == adquisicion[1]:
                row['Tipo de Adq'] = adquisicion[0]
            else:
                row['Tipo de Adq'] = 2
        for valuacion in inv_valuaciones:
            if val == valuacion[1]:
                row['Tipo de Valuacion'] = valuacion[0]
            elif val == 'nan':
                row['Tipo de Valuacion'] = 1
        for programa in cat_programas:
            if program == programa[1]:
                row['PROGRAMA'] = programa[0]
            else:
                row['PROGRAMA'] = 1
        for especialidad in inv_especialidades:
            if espe == especialidad[1]:
                row['especialidad'] = especialidad[0]
            else:
                row['especialidad'] = 93
        lista.append(row)
    return lista


def get_format_bienes(lista):
    bienes = []
    print('Creating instance for insert in inv_bienes...')
    for row in lista:
        # CARACTERISTICAS
        row['CLAVE_CAE'] = "'" + format_clave_cae(str(row['BMI_CTA_CON']),str(row['BMI_INV'])) + "'"
        row['BMI_INV'] = "'" + format_clave_cae('', str(row['BMI_INV'])) + "'"
        row['ID_SESVER'] = format_string_to_null(str(int(row['ID_SESVER'])) if str(row['ID_SESVER']) != 'nan' else str(row['ID_SESVER']))
        row['Fecha de Entrega'] = format_fecha(row['Fecha de Entrega'])
        row['BMI_FEC_OFBAJA'] = format_fecha(row['BMI_FEC_OFBAJA'])
        row['BMI_OFI'] = format_string_to_null(str(row['BMI_OFI']))
        row['Motivo baja'] = format_string_to_null(str(row['Motivo baja']))
        row['Inventariado'] = format_fecha(row['Inventariado'])
        row['Dado de Baja'] = format_number_to_status(row['Dado de Baja'])
        # DOCUMENTO
        row['BMI_FAD'] = format_fecha(row['BMI_FAD'])
        row['BMI_FAC'] = format_string_to_null(str(row['BMI_FAC']))
        row['BMI_COS'] = format_number_to_decimal(float(row['BMI_COS']))
        row['Folio'] = format_string_to_null(str(row['Folio']))
        # DESCRIPCION
        row['BMI_MARCA'] = format_string_to_null(str(row['BMI_MARCA']))
        row['BMI_MOD'] = format_string_to_null(str(row['BMI_MOD']))
        row['BMI_SER'] = format_string_to_null(str(row['BMI_SER']))
        row['NoCatálogo'] = format_string_to_null(str(row['NoCatálogo']))
        row['BMI_MED'] = format_string_to_null(str(row['BMI_MED']))
        row['BMI_EST'] = format_string_to_null(str(row['BMI_EST']))
        row['BMI_COL'] = format_string_to_null(str(row['BMI_COL']))
        row['NoCharola'] = format_string_to_null(str(row['NoCharola']))
        row['BMI_OBS'] = format_string_to_null(str(row['BMI_OBS']))
        #TIMESTAPS
        row['BMI_FAL'] = format_fecha(row['BMI_FAL'])
        row['BMI_FBA'] = format_fecha(row['BMI_FBA'])
        row['BMI_DOC_TIPO'] = 1
        bienes.append(row)
    con_mueble = convert_mueble_id(bienes)
    con_catalogos = convert_catalogos_inventarios(con_mueble)
    return con_catalogos


def insert_bienes():
    bienes = get_dic_excel('MIGRACION.xlsx', 'MIGRACION')
    instance = get_format_bienes(bienes)
    base_os = get_connection_mysql('10.30.13.6', 'db_sica', 'root', 'siscae1035')
    cursorq = base_os.cursor(buffered=True)
    cursorq.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cursorq.execute("TRUNCATE TABLE inv_bienes")
    cursorq.execute("SET FOREIGN_KEY_CHECKS = 1;")
    f = open("inv_bienes.sql", "w")
    for ins in instance:
        print(f"INSERT INTO inv_bienes (clave_cae, num_bien, id_sesver, fecha_entrega, fecha_oficio_baja, oficio_baja, motivo_baja, inventariado, status_bien, doc_fecha, doc_num, doc_precio, doc_folio, doc_proveedor, des_marca, des_modelo, des_num_serie, des_num_catalogo, des_medidas, des_estructura, des_color, des_nombre, des_num_charola, des_observaciones, id_tipo_mueble, id_ubicacion, id_doc_tipo, id_doc_adquisicion, id_doc_valuacion, id_doc_programa, id_des_especialidad, created_at, updated_at, deleted_at) VALUES({ins['CLAVE_CAE']}, {ins['BMI_INV']}, {ins['ID_SESVER']}, {ins['Fecha de Entrega']}, {ins['BMI_FEC_OFBAJA']}, {ins['BMI_OFI']}, {ins['Motivo baja']}, {ins['Inventariado']}, {ins['Dado de Baja']}, {ins['BMI_FAD']}, {ins['BMI_FAC']}, {ins['BMI_COS']}, {ins['Folio']}, {ins['BMI_PRO']}, {ins['BMI_MARCA']}, {ins['BMI_MOD']}, {ins['BMI_SER']}, {ins['NoCatálogo']}, {ins['BMI_MED']}, {ins['BMI_EST']}, {ins['BMI_COL']}, {ins['DES_NOMBRE']}, {ins['NoCharola']}, {ins['BMI_OBS']}, {ins['ID_TIPO_MUEBLE']}, {ins['BMI_AREA']}, {ins['BMI_DOC_TIPO']}, {ins['Tipo de Adq']}, {ins['Tipo de Valuacion']}, {ins['PROGRAMA']}, {ins['especialidad']}, {ins['BMI_FAL']}, {'now()'}, {ins['BMI_FBA']});", file=f)
        cursorq.execute(f"INSERT INTO inv_bienes (clave_cae, num_bien, id_sesver, fecha_entrega, fecha_oficio_baja, oficio_baja, motivo_baja, inventariado, status_bien, doc_fecha, doc_num, doc_precio, doc_folio, doc_proveedor, des_marca, des_modelo, des_num_serie, des_num_catalogo, des_medidas, des_estructura, des_color, des_nombre, des_num_charola, des_observaciones, id_tipo_mueble, id_ubicacion, id_doc_tipo, id_doc_adquisicion, id_doc_valuacion, id_doc_programa, id_des_especialidad, created_at, updated_at, deleted_at) VALUES({ins['CLAVE_CAE']}, {ins['BMI_INV']}, {ins['ID_SESVER']}, {ins['Fecha de Entrega']}, {ins['BMI_FEC_OFBAJA']}, {ins['BMI_OFI']}, {ins['Motivo baja']}, {ins['Inventariado']}, {ins['Dado de Baja']}, {ins['BMI_FAD']}, {ins['BMI_FAC']}, {ins['BMI_COS']}, {ins['Folio']}, {ins['BMI_PRO']}, {ins['BMI_MARCA']}, {ins['BMI_MOD']}, {ins['BMI_SER']}, {ins['NoCatálogo']}, {ins['BMI_MED']}, {ins['BMI_EST']}, {ins['BMI_COL']}, {ins['DES_NOMBRE']}, {ins['NoCharola']}, {ins['BMI_OBS']}, {ins['ID_TIPO_MUEBLE']}, {ins['BMI_AREA']}, {ins['BMI_DOC_TIPO']}, {ins['Tipo de Adq']}, {ins['Tipo de Valuacion']}, {ins['PROGRAMA']}, {ins['especialidad']}, {ins['BMI_FAL']}, {'now()'}, {ins['BMI_FBA']});")
    f.close()
    base_os.commit()
    cursorq.close()
    base_os.close()
    print('inv_bienes migrated successfully')
