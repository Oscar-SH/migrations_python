import script_bienes
import script_cat_mueble
import script_resguardos

if __name__ == "__main__":
    print("Running inv_cat_mueble migration...")
    script_cat_mueble.insert_muebles()
    print("Running inv_bienes migration...")
    script_bienes.insert_bienes()
    print("Running inv_bienes_resguardos migration...")
    script_resguardos.insert_bienes_resguardos()
    print('OK the migrations have been run successfully')