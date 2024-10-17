import socket  
import time  
import random  
  
# Função que simula o envio de uma requisição de um cliente para um nó do cluster
def send_request(client_id, timestamp):
    cluster_node = ("127.0.0.1", 5002)
    
    # Cria um socket para se conectar ao nó escolhido
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect(cluster_node)        
        request = f"{client_id},{timestamp}"
        print(f"A requisição sendo enviado para o nó é ClientId = {client_id} e Timestamp = {timestamp}\n")
        
        client_socket.send(request.encode())

        response = client_socket.recv(1024).decode()
        print(f"Cliente {client_id} recebeu resposta: {response}")  # Exibe a resposta
    
    except ConnectionRefusedError as e:
        print(f"Conexão recusada com o nó {cluster_node}\n")
        client_socket.close()
        send_request(client_id, timestamp)

    finally:
        client_socket.close()


if __name__ == "__main__":
    client_id = "Client2"
    
    id = 0
    # O cliente faz entre 10 e 50 requisições ao Cluster Sync
    for _ in range(random.randint(10, 50)):
        print(f"**********************************\nRequisição {id}:")
        timestamp = int(time.time() * 1000)
        print(f"Timestamp gerado foi de {timestamp}\n")
        send_request(client_id, timestamp)  
        time.sleep(random.uniform(1,5))
        id = id + 1
