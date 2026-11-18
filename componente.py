#!/usr/bin/env python3

# Componentes Basico
# PD/SDC - PUCPR
# Prof. Luiz Lima Jr.

from pika import BlockingConnection
from sys import argv
from enum import Enum

if len(argv) < 2:
    print(f"USO: {argv[0]} <id> [<v1> <v2> ...]")
    exit(1)

idx = argv[1]
Nx = argv[2:]
nao_recebidos = Nx[:]
entrada = None
informacao = idx

Estado = Enum("Estado", "INICIADOR OCIOSO AGUARDANDO OK")

estado = Estado.OCIOSO

print("meu id =", idx)
print("meus vizinhos =", Nx)

conexao = BlockingConnection()
canal = conexao.channel()

canal.queue_declare(queue=idx, durable=True, auto_delete=True)

# envia mensagem para destinos
def envia(msg, dest):
    canal.basic_publish(exchange="", routing_key=dest, body=f"{idx}:{msg}")
    print(f"enviando {msg} para {dest}")

def concluiu():
    global estado
    if nao_recebidos:
        return
    if estado == Estado.INICIADOR:
        print("onda concluida. informacao:", informacao)
    else:
        print("recebi de todos os vizinhos")
        envia("I" + informacao, entrada)
    estado = Estado.OK

def espontaneamente(msg):
    global estado, nao_recebidos
    if estado != Estado.OCIOSO:
        return
    print("iniciador:", msg)
    estado = Estado.INICIADOR
    nao_recebidos = Nx[:]
    for vizinho in Nx:
        envia("R", vizinho)
    concluiu()

def recebendo(msg, origem):
    global estado, entrada, nao_recebidos, informacao
    tipo, dado = msg[:1], msg[1:]
    print(f"recebi {tipo} de {origem}")
    if estado == Estado.OCIOSO:
        if tipo == "R":
            entrada = origem
            nao_recebidos = [v for v in Nx if v != origem]
            estado = Estado.AGUARDANDO
            for vizinho in nao_recebidos:
                envia("R", vizinho)
            concluiu()
    elif estado in (Estado.INICIADOR, Estado.AGUARDANDO):
        if tipo in ("R", "I") and origem in nao_recebidos:
            nao_recebidos.remove(origem)
            if tipo == "I":
                informacao += "," + dado
                print("informacao: ", informacao)
            concluiu()

def callback(canal, metodo,  props, msg):
    m = msg.decode().split(":", 1)
    if len(m) < 2:
        origem = "STARTER"
        msg = m[0]
    else:
        origem = m[0]
        msg = m[1]
    if origem.upper() == "STARTER":
        espontaneamente(msg)
    else:
        recebendo(msg, origem)


canal.basic_consume(queue=idx,
                    on_message_callback=callback,
                    auto_ack=True)

try:
    print(f"{idx}: aguardando mensagens...")
    canal.start_consuming()
except KeyboardInterrupt:
    canal.stop_consuming()

conexao.close()
print("Terminando")
