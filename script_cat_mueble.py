import pandas as pd
import mysql.connector
from datetime import datetime


def get_connection_mysql(ip: str, db: str, usuario: str, passw: str):
    cnxn = mysql.connector.connect(host=ip, database=db, user=usuario, password=passw)
    print(f'Connecting to {ip} in {db}')
    return cnxn


def get_query(query: str):
    result = get_connection_mysql(
        '10.30.13.37', 'inventarios', 'root', 'siscae1035')
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


def text_to_null(text: str):
    if text == "nan":
        valor = 'NULL'
    else:
        valor = f"'{text}'"
    return valor


def transalate_new_capitulo():
    cat_mueble = get_dic_excel('BMCCMUEB.xlsx', 'BMCCMUEB')
    lista = []
    for row in cat_mueble:
        if str(row['CAPITULO SESVER']) == 'nan':
            row['NUEVA_CLAVE'] = 'NULL'
            row['CAPITULO'] = 'NULL'
        else:
            if str(int(row['CAPITULO SESVER'])) == "1":
                row['NUEVA_CLAVE'] = 'NULL'
                row['CAPITULO'] = 'NULL'
            elif str(int(row['CAPITULO SESVER'])) == "5101":
                row['NUEVA_CLAVE'] = '51100001'
                row['CAPITULO'] = 'MOBILIARIO DE OFICINA'
            elif str(int(row['CAPITULO SESVER'])) == "5102":
                row['NUEVA_CLAVE'] = '52000000'
                row['CAPITULO'] = 'EQUIPO DE ADMINISTRACION'
            elif str(int(row['CAPITULO SESVER'])) == "5103":
                row['NUEVA_CLAVE'] = '56200001'
                row['CAPITULO'] = 'EQUIPO EDUCACIONAL Y RECREATIVO'
            elif str(int(row['CAPITULO SESVER'])) == "5104":
                row['NUEVA_CLAVE'] = '56600001'
                row['CAPITULO'] = 'BIENES ARTISTICOS Y CULTURALES'
            elif str(int(row['CAPITULO SESVER'])) == "5202":
                row['NUEVA_CLAVE'] = '53100001'
                row['CAPITULO'] = 'MAQUINARIA Y EQUIPO INDUSTRIAL'
            elif str(int(row['CAPITULO SESVER'])) == "5204":
                row['NUEVA_CLAVE'] = '56700002'
                row['CAPITULO'] = 'EQUIPOS Y APARATOS DE COMUNICACIONES'
            elif str(int(row['CAPITULO SESVER'])) == "5205":
                row['NUEVA_CLAVE'] = '54100003'
                row['CAPITULO'] = 'MAQUINARIA Y EQUIPO ELECTRICO Y ELECTRONICO'
            elif str(int(row['CAPITULO SESVER'])) == "5206":
                row['NUEVA_CLAVE'] = '51300000'
                row['CAPITULO'] = 'BIENES INFORMATICOS'
            elif str(int(row['CAPITULO SESVER'])) == "5401":
                row['NUEVA_CLAVE'] = '51500001'
                row['CAPITULO'] = 'EQUIPO MEDICO  Y DE LABORATORIO'
            elif str(int(row['CAPITULO SESVER'])) == "5402":
                row['NUEVA_CLAVE'] = '54100002'
                row['CAPITULO'] = 'INSTRUMENTAL MEDICO Y DE LABORATORIO'
            elif str(int(row['CAPITULO SESVER'])) == "5501":
                row['NUEVA_CLAVE'] = '51900001'
                row['CAPITULO'] = 'HERRAMIENTAS Y MAQUINAS HERRAMIENTAS'
            elif str(int(row['CAPITULO SESVER'])) == "5303":
                row['NUEVA_CLAVE'] = '53200001'
                row['CAPITULO'] = 'VEHICULOS Y EQUIPOS TERRESTRES, AEREOSMARITIMOS, LACUESTRES Y FLUVIALES DESTINAD'
            elif str(int(row['CAPITULO SESVER'])) == "5304":
                row['NUEVA_CLAVE'] = '56500001'
                row['CAPITULO'] = 'VEHICULOS Y EQS TERRESTRES, AEREOS MARITIMOS, LACUSTRES Y FLUV. PARA SERV.ADTIVO'
            elif str(int(row['CAPITULO SESVER'])) == "5502":
                row['CAPITULO SESVER'] = '5501'
                row['NUEVA_CLAVE'] = '51900001'
                row['CAPITULO'] = 'HERRAMIENTAS Y MAQUINAS HERRAMIENTAS'
        lista.append(row)
    print('Translating capitulo to nueva_clave')
    return lista


def capitulo_to_null(text: str):
    if text == 'nan':
        res = 'NULL'
    else:
        res = f"'{str(int(float(text)))}'"
    return res


def fomat_text(bienes):
    lista = []
    for row in bienes:
        row['BMI_CTA_CON'] = text_to_null(str(row['BMI_CTA_CON']))
        des = str(row['BMM_DES']).replace("'", "")
        des = des.replace("´", "")
        row['BMM_DES'] = text_to_null(des)
        row['Cuenta Contable SSA'] = text_to_null(str(row['Cuenta Contable SSA']))
        row['CAPITULO SESVER'] = capitulo_to_null(str(row['CAPITULO SESVER']))
        row['CAPITULO'] = text_to_null(str(row['CAPITULO']))
        row['NUEVA_CLAVE'] = text_to_null(str(row['NUEVA_CLAVE']))
        lista.append(row)
    print('Creating instance for insert in db')
    return lista


def insert_muebles():
    base_os = get_connection_mysql('10.30.13.6', 'db_sica', 'root', 'siscae1035')
    cursorq = base_os.cursor(buffered=True)
    cursorq.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cursorq.execute("TRUNCATE TABLE inv_cat_mueble")
    cursorq.execute("SET FOREIGN_KEY_CHECKS = 1;")
    caps = transalate_new_capitulo()
    instance = fomat_text(caps)
    f = open("inv_cat_mueble.sql", "w")
    for ins in instance:
        print(f"INSERT INTO inv_cat_mueble (clave, nombre_mueble, cuenta_ssa, clave_capitulo, capitulo, created_at, updated_at, deleted_at, nueva_clave_capitulo) VALUES({ins['BMI_CTA_CON']}, {ins['BMM_DES']}, {ins['Cuenta Contable SSA']}, {ins['CAPITULO SESVER']}, {ins['CAPITULO']}, {'now()'}, {'now()'}, {'NULL'}, {ins['NUEVA_CLAVE']});", file=f)
        cursorq.execute(f"INSERT INTO inv_cat_mueble (clave, nombre_mueble, cuenta_ssa, clave_capitulo, capitulo, created_at, updated_at, deleted_at, nueva_clave_capitulo) VALUES({ins['BMI_CTA_CON']}, {ins['BMM_DES']}, {ins['Cuenta Contable SSA']}, {ins['CAPITULO SESVER']}, {ins['CAPITULO']}, {'now()'}, {'now()'}, {'NULL'}, {ins['NUEVA_CLAVE']});")
    f.close()
    print ('Closing database and text files.')
    base_os.commit()
    cursorq.close()
    base_os.close()
    print('inv_cat_mueble migrated successfully.\n')
