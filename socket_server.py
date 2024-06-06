import socket

# Configuración del servidor
HOST = 'localhost'  # Dirección IP del servidor
PORT = 65432        # Puerto del servidor

# Crear el socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen()

print(f"Servidor escuchando en {HOST}:{PORT}")

# Aceptar conexiones
conn, addr = server_socket.accept()
print(f"Conexión aceptada de {addr}")
with conn:
    print(f"Conectado por {addr}")
    while True:
        data = conn.recv(1024)
        if not data:
            break
        print(f"Recibido: {data.decode()}")
