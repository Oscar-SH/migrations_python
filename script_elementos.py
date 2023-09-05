import pandas as pd
import mysql.connector
from datetime import datetime
from hermetrics.hamming import Hamming


def get_connection_mysql(ip: str, db: str, usuario: str, passw: str):
    cnxn = mysql.connector.connect(
        host=ip, database=db, user=usuario, password=passw)
    # print(f'Connecting to {ip} in {db}')
    return cnxn


def get_query(query: str):
    result = get_connection_mysql(
        '10.30.13.6', 'db_sica', 'root', 'siscae1035')
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


def convert_clave(clave: str, consecutivo: str, elemento: str):
    if clave:
        clave = str(int(clave))
    if consecutivo:
        consecutivo = str(int(consecutivo))
    elemento = str(int(elemento))
    if len(consecutivo) == 1:
        numero = '00' + consecutivo
    elif len(consecutivo) == 2:
        numero = '0' + consecutivo
    else:
        numero = consecutivo

    if len(elemento) == 1:
        numeroel = '00' + elemento
    elif len(elemento) == 2:
        numeroel = '0' + elemento
    else:
        numeroel = elemento

    clave_cae = clave[0:4] + '-' + clave[4:7] + '-' + numero + '/' + numeroel

    return f"'{clave_cae}'" if clave else f"'{numeroel}'"


def convert_string_to_null(text: str):
    if text == 'nan':
        textc = 'NULL'
    else:
        textc = f"'{text}'"

    return textc


def transport_data_bien_to_elemento(bienes):
    inv_bienes = get_query(f"SELECT * FROM inv_bienes")
    inv_cat_muebles = get_query(f"SELECT * FROM inv_cat_mueble")
    lista = []
    for row in bienes:
        clv = row['BME_CT2_CO2']
        clvse = clv[1:5] + clv[6:9]
        for bien in inv_bienes:
            if bien[1] == clv[1:13]:
                row['INV_STATUS'] = f"'{bien[9]}'"
                row['ID_UBICACION'] = bien[-9]
                row['ID_BIEN'] = bien[0]
                row['BMI_FAL'] = f"'{bien[-3]}'"
        for mueble in inv_cat_muebles:
            if mueble[1] == clvse:
                row['DES_NOMBRE'] = f"'{mueble[2]}'"
                row['ID_MUEBLE'] = mueble[0]
        lista.append(row)

    return lista


def create_instance(elementos):
    lista = []
    for row in elementos:
        clave_cae = convert_clave(
            row['BME_CTA_CON'], row['BME_INV'], row['BME_ELE'])
        row['BME_CT2_CO2'] = clave_cae
        row['BME_ELE'] = convert_clave('', '', row['BME_ELE'])
        row['BME_MARCA'] = convert_string_to_null(str(row['BME_MARCA']))
        row['BME_MOD'] = convert_string_to_null(str(row['BME_MOD']))
        row['BME_SER'] = convert_string_to_null(str(row['BME_SER']))
        row['NUM_CATALOGO'] = 'NULL'
        row['BME_MED'] = convert_string_to_null(str(row['BME_MED']))
        row['BME_EST'] = convert_string_to_null(str(row['BME_EST']))
        row['BME_COL'] = convert_string_to_null(str(row['BME_COL']))
        lista.append(row)
    con_herencia = transport_data_bien_to_elemento(lista)
    return con_herencia


def insert_elementos_bienes():
    elementos = get_dic_excel('BMINELEM.xlsx', 'BMINELEM')
    instance = create_instance(elementos)
    base_os = get_connection_mysql(
        '10.30.13.6', 'db_sica', 'root', 'siscae1035')
    cursorq = base_os.cursor(buffered=True)
    cursorq.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cursorq.execute("TRUNCATE TABLE inv_elementos_bienes")
    cursorq.execute("SET FOREIGN_KEY_CHECKS = 1;")
    f = open("inv_elementos_bienes.sql", "w")
    claves = []
    for ins in instance:
        if ins['BME_CT2_CO2'] in claves:
            pass
        else:
            claves.append(ins['BME_CT2_CO2'])
            print(f"INSERT INTO inv_elementos_bienes ( clave_cae, num_elemento, status_bien, des_marca, des_modelo, des_num_serie, des_num_catalogo, des_medidas, des_estructura, des_color, des_nombre, id_ubicacion, id_tipo_mueble, id_bien, created_at, updated_at) VALUES({ins['BME_CT2_CO2']}, {ins['BME_ELE']}, {ins['INV_STATUS']}, {ins['BME_MARCA']}, {ins['BME_MOD']}, {ins['BME_SER']}, {ins['NUM_CATALOGO']},{ins['BME_MED']},{ins['BME_EST']},{ins['BME_COL']},{ins['DES_NOMBRE']},{ins['ID_UBICACION']}, {ins['ID_MUEBLE']},{ins['ID_BIEN']}, {ins['BMI_FAL']}, {'now()'});", file=f)
            cursorq.execute( f"INSERT INTO inv_elementos_bienes ( clave_cae, num_elemento, status_bien, des_marca, des_modelo, des_num_serie, des_num_catalogo, des_medidas, des_estructura, des_color, des_nombre, id_ubicacion, id_tipo_mueble, id_bien, created_at, updated_at) VALUES({ins['BME_CT2_CO2']}, {ins['BME_ELE']}, {ins['INV_STATUS']}, {ins['BME_MARCA']}, {ins['BME_MOD']}, {ins['BME_SER']}, {ins['NUM_CATALOGO']},{ins['BME_MED']},{ins['BME_EST']},{ins['BME_COL']},{ins['DES_NOMBRE']},{ins['ID_UBICACION']}, {ins['ID_MUEBLE']},{ins['ID_BIEN']}, {ins['BMI_FAL']}, {'now()'});")
    f.close()
    base_os.commit()
    cursorq.close()
    base_os.close()
    print('inv_elementos_bienes migrated successfully')
