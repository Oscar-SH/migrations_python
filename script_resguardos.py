import pandas as pd
import mysql.connector
from datetime import datetime
from hermetrics.hamming import Hamming


def get_connection_mysql(ip: str, db: str, usuario: str, passw: str):
    cnxn = mysql.connector.connect(
        host=ip, database=db, user=usuario, password=passw)
    print(f'Connecting to {ip} in {db}')
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


def is_in_catalog(text: str, catalogo, pb, pr):
    for x in catalogo:
        if text == x[pb]:
            return x[pr]
    return 'NULL'


def format_clave(text: str, consecutive: str):
    if len(consecutive) == 1:
        numero = '00'+consecutive
    elif len(consecutive) == 2:
        numero = '0'+consecutive
    else:
        numero = consecutive
    clave = text[0:4] + '-' + text[4:7] + '-' + numero
    return clave


def create_instance(resguardos):
    lista = []
    rch_empleados = get_query("SELECT * FROM rch_empleados")
    inv_bienes = get_query("SELECT * FROM inv_bienes")
    for row in resguardos:
        clv = format_clave(str(row['BMR_CTA_CON']), str(row['BMR_INV']))
        row['BMR_BIEN'] = is_in_catalog(clv, inv_bienes, 1, 0)
        row['BMR_EMP'] = is_in_catalog(row['BMR_NPERS'], rch_empleados, 1, 0)
        lista.append(row)
    return lista


def insert_bienes_resguardos():
    resguardos = get_dic_excel('BMINRESG.xlsx', 'BMINRESG')
    instance = create_instance(resguardos)
    base_os = get_connection_mysql(
        '10.30.13.6', 'db_sica', 'root', 'siscae1035')
    cursorq = base_os.cursor(buffered=True)
    cursorq.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cursorq.execute("TRUNCATE TABLE inv_bienes_resguardos")
    cursorq.execute("SET FOREIGN_KEY_CHECKS = 1;")
    f = open("inv_bienes_resguardos.sql", "w")
    for ins in instance:
        if ins['BMR_BIEN'] != 'NULL' and ins['BMR_EMP'] != 'NULL':
            if ins['BMR_EMP'] != 1:
                print(
                    f"INSERT INTO inv_bienes_resguardos ( id_bien, id_empleado, created_at, updated_at) VALUES({ins['BMR_BIEN']}, {ins['BMR_EMP']}, {'now()'}, {'now()'});", file=f)
                cursorq.execute(
                    f"INSERT INTO inv_bienes_resguardos ( id_bien, id_empleado, created_at, updated_at) VALUES({ins['BMR_BIEN']}, {ins['BMR_EMP']}, {'now()'}, {'now()'});")
    f.close()
    base_os.commit()
    cursorq.close()
    base_os.close()
    print('inv_bienes_resguardos migrated successfully')
