import subprocess
import concurrent.futures
import time
import threading

def run_script_forever(script_name):
    while True:
        process = subprocess.Popen(["python3", script_name])
        process.wait()

def run_script(script_name):
    process = subprocess.Popen(["python3", script_name])
    process.wait()

def main():
    try:
        # Ejecutar generate_data.py y process_data.py en bucles infinitos
        with concurrent.futures.ThreadPoolExecutor() as executor:
            print("Ejecutando generate_data.py indefinidamente...")
            future_generate = executor.submit(run_script_forever, "generate_data.py")
            
            print("Ejecutando process_data.py indefinidamente...")
            future_process = executor.submit(run_script_forever, "process_data.py")
            
            def periodic_tasks():
                while True:
                    time.sleep(30)  # Esperar 30 segundos

                    # Ejecutar bronze_tabla_SQL.py
                    print("Ejecutando bronze_tabla_SQL.py...")
                    run_script("bronze_tabla_SQL.py")
                    
                    # Ejecutar bronze_to_silver.py y empleados_clientes_silver.py simultáneamente
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        print("Ejecutando bronze_to_silver.py...")
                        future_bronze_to_silver = executor.submit(run_script, "bronze_to_silver.py")
                        
                        print("Ejecutando empleados_clientes_silver.py...")
                        future_empleados_clientes_silver = executor.submit(run_script, "silver_empleados_clientes.py")
                        
                        concurrent.futures.wait([future_bronze_to_silver, future_empleados_clientes_silver])

                    # Ejecutar silver_tabla_SQL.py
                    print("Ejecutando silver_tabla_SQL.py...")
                    run_script("silver_tabla_SQL.py")
                    
                    # Ejecutar gold_data.py al final
                    print("Ejecutando gold_data.py...")
                    run_script("gold_data.py")

                    print("Orquestación completada.")

            # Ejecutar tareas periódicas cada 30 segundos en un hilo separado
            periodic_thread = threading.Thread(target=periodic_tasks)
            periodic_thread.daemon = True
            periodic_thread.start()

            # Mantener el programa principal corriendo para que los hilos puedan seguir ejecutándose
            future_generate.result()
            future_process.result()

    except Exception as e:
        print(f"Error en la orquestación: {e}")

if __name__ == "__main__":
    main()