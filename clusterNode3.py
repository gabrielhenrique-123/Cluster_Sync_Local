import socket
import threading
import time
import random
import json
from dataclasses import dataclass

# Definindo a estrutura do nó do cluster
@dataclass
class ClusterNode:
    ip: str
    port: int
    node_id: str
    client_id: str
    oks_received: int = 0
    timestamp: int = None
    accessing_resource: bool = False  # Indica se está acessando o recurso
    wants_resource: bool = False  # Indica se deseja acessar o recurso
    queue: list = None  # Fila de requisições pendentes

    def __post_init__(self):
        self.queue = []

# Instâncias dos nós do cluster
cluster1 = ClusterNode("127.0.0.1", 6001, "Peer1", "Client1")
cluster2 = ClusterNode("127.0.0.1", 6002, "Peer2", "Client2")
local_cluster = ClusterNode("127.0.0.1", 6003, "Peer3", "Client3")
cluster4 = ClusterNode("127.0.0.1", 6004, "Peer4", "Client4")
cluster5 = ClusterNode("127.0.0.1", 6005, "Peer5", "Client5")

# Lista de nós do cluster
cluster_nodes = [cluster1, cluster2]

process_count = 0  # Contador para o número de processos

#-------------------------------------------------------------------CLIENT-------------------------------------------------------------------------------
def node_server(host, port, local_node_id):
    """
    Servidor principal que recebe requisições dos clientes.
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)

    print(f"Nó {local_node_id} rodando em {host}:{port}")

    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conexão recebida de {addr}")

        client_handler = threading.Thread(target=handle_client_request, args=(client_socket, addr, local_node_id))
        client_handler.start()

def handle_client_request(client_socket, address, local_node_id):
    """
    Processa requisições dos clientes e propaga para o cluster.
    """
    global process_count
    process_count += 1  
    data = client_socket.recv(1024).decode()

    # Verificar se os dados contêm uma vírgula antes de fazer o split
    if ',' in data:
        client_id, timestamp = data.split(',')
        timestamp = int(timestamp)

        local_cluster.timestamp = timestamp
        local_cluster.wants_resource = True

        local_request = {
            'node_id': local_node_id,
            'client_id': client_id,
            'timestamp': timestamp
        }

        print(f"Processo {process_count}: Requisição de {client_id} recebida com timestamp {timestamp}")
        propagate_to_cluster(local_request)
        print(f"Processo {process_count}: OKs recebidos: {local_cluster.oks_received}")
        request_id = f"{client_id}_{timestamp}"

        wait_for_oks(request_id, local_cluster.oks_received)

        process_critical_section(local_request)

        local_cluster.wants_resource = False

        if local_node_id == local_cluster.node_id:
            client_socket.send(f"COMMITTED for {client_id} at timestamp {timestamp}".encode())
    else:
        # Tratamento para OK ou outras respostas
        if data == "OK":
            local_cluster.oks_received += 1
            print(f"Processo {process_count}: OK recebido de {address}")
        else:
            print(f"Processo {process_count}: Mensagem inesperada recebida: {data}")

    client_socket.close()

    # Envia OK para processos na fila após sair da seção crítica
    finish_critical_section(local_cluster)


def propagate_to_cluster(request):
    """
    Propaga a requisição atual para os outros nós do Cluster Sync.
    """
    local_cluster.oks_received = 0
    for node in cluster_nodes:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
            try:
                node_socket.connect((node.ip, node.port))  # Conexão ao nó
                node_socket.send(json.dumps(request).encode())
                ack = node_socket.recv(1024).decode()
                if(ack == "WAIT"):
                    while True:
                        if node_socket.recv(1024).decode() == "OK":
                            break
                print(f"Processo {process_count}: {ack} de {node.node_id} recebido")
                local_cluster.oks_received += 1
            except Exception as e:
                print(f"Processo {process_count}: Falha ao conectar com {node.node_id}: {e}")
  
def process_critical_section(request):
    """
    Simula o processamento na seção crítica, garantindo que apenas um cliente por vez
    possa processar sua requisição.
    """
    local_cluster.accessing_resource = True
    print(f"Processo {process_count}: {request['client_id']} entrou na seção crítica")
    time.sleep(random.uniform(0.2, 1)) 
    print(f"Processo {process_count}: {request['client_id']} saiu da seção crítica\n")
    local_cluster.accessing_resource = False

def wait_for_oks(request_id, oks_received):
    """
    Aguarda a recepção de todos os "OKs" dos nós do Cluster Sync.
    """
    while True:
        if oks_received == len(cluster_nodes):
            break
        time.sleep(0.1)

    print(f"Processo {process_count}: Todos os OKs recebidos para a requisição {request_id}. Entrando na seção crítica...")

def finish_critical_section(local_cluster):
    """
    Função chamada quando o processo termina de acessar a seção crítica.
    Envia OK para todos os processos que estão aguardando na fila.
    """
    # Enquanto houver processos aguardando na fila
    while local_cluster.queue:
        queued_request = local_cluster.queue.pop(0)  # Remove a requisição mais antiga da fila
        send_ok_to(queued_request["client_id"])  # Envia OK para o cliente da requisição

    print(f"Processo {process_count}: OKs enviados para todos os processos na fila.")

def send_ok_to(client_id):
    """
    Envia uma mensagem OK para o cliente que solicitou acesso ao recurso.
    """
    # Procura o nó correspondente ao client_id
    send_cluster = None
    for cluster in cluster_nodes:
        if cluster.client_id == client_id:
            send_cluster = cluster
            break
    
    # Verifica se encontrou o nó correto
    if send_cluster:
        try:
            # Envia um OK para o nó (usando a porta correta)
            send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_socket.connect((send_cluster.ip, send_cluster.port-1000))
            send_socket.send("OK".encode())
            send_socket.close()
            print(f"Processo {process_count}: OK enviado para o nó associado ao {client_id}")
        except Exception as e:
            print(f"Erro ao enviar OK para {client_id}: {e}")
    else:
        print(f"Nó associado ao {client_id} não encontrado.")
#-------------------------------------------------------------------CLUSTER-------------------------------------------------------------------------------

def cluster_sync_server(host, port, local_node_id):
    """
    Servidor que lida com requisições propagadas de outros nós do Cluster Sync.
    """
    node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_socket.bind((host, port))
    node_socket.listen(5)

    print(f"Servidor de sincronização {local_node_id} rodando em {host}:{port}")

    while True:
        node_connection, addr = node_socket.accept()
        print(f"Conexão de sincronização recebida de {addr}")

        request_data = node_connection.recv(1024).decode()
        request_received = handle_propagated_request(request_data)  
        print(f"Requisição recebida de {request_received['client_id']} com timestamp {request_received['timestamp']}")

        if process_request(request_received, local_cluster):
            node_connection.send("OK".encode())
        else:
            node_connection.send("WAIT".encode())

        node_connection.close()

def handle_propagated_request(request_data):
    """
    Processa requisições propagadas de outros nós do cluster.
    """
    request = json.loads(request_data)  
    return request

def process_request(request_received, local_cluster):
    """
    Processa a requisição recebida de outro nó e toma ação baseada nas regras do algoritmo.
    """

    send_ok = False

    sender_client_id = request_received["client_id"]
    sender_timestamp = request_received["timestamp"]

    # Se o receptor não estiver acessando o recurso e não quiser acessá-lo (regra 1)
    if not local_cluster.accessing_resource and not local_cluster.wants_resource:
        send_ok = True
        #send_ok_to(sender_client_id)  # Envia OK ao remetente

    # Se o receptor já estiver acessando o recurso (regra 2)
    elif local_cluster.accessing_resource:
        local_cluster.queue.append(request_received)  # Enfileira a requisição
        send_ok = True

    # Se o receptor também quiser acessar o recurso (regra 3)
    elif local_cluster.wants_resource:
        # Compara os timestamps: o menor vence
        if sender_timestamp < local_cluster.timestamp:
            send_ok = True
          #  send_ok_to(sender_client_id)  # Envia OK ao remetente
        else:
            local_cluster.queue.append(request_received)  # Enfileira a requisição
            send_ok = True
    
    return send_ok

#-------------------------------------------------------------------MAIN-------------------------------------------------------------------------------

if __name__ == "__main__":
    node_id = "Peer3"  
    threading.Thread(target=node_server, args=("127.0.0.1", 5003, node_id)).start()
    threading.Thread(target=cluster_sync_server, args=("127.0.0.1", 6003, node_id)).start()
