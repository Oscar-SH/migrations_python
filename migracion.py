import script_bienes
import script_cat_mueble
import script_resguardos
import script_elementos

if __name__ == "__main__":
    print("Running inv_cat_mueble migration...")
    script_cat_mueble.insert_muebles()
    print("Running inv_bienes migration...")
    script_bienes.insert_bienes()
    print("Running inv_bienes_resguardos migration...")
    script_resguardos.insert_bienes_resguardos()
    print("Running inv_elementos_bienes migration...")
    script_elementos.insert_elementos_bienes()
    print('OK the migrations have been run successfully')