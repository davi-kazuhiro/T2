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
iniciador = False
entrada = None
informacao = ""

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

def espontaneamente(msg):
    global estado, iniciador, nao_recebidos, informacao
    print("iniciador:", msg)
    estado = Estado.INICIADOR
    if estado == Estado.INICIADOR:
        iniciador = True
        nao_recebidos = Nx[:]
        for vizinho in Nx:
            envia("R", vizinho)
        estado = Estado.AGUARDANDO

def recebendo(msg, origem):
    global estado, entrada, iniciador, nao_recebidos, informacao
    if iniciador:
        if(msg[0] == "R"):
            print(f"recebi R de {origem}")
            nao_recebidos.remove(origem)
            if len(nao_recebidos) == 0:
                print("todos vizinhos responderam, enviando token\ninformacao:", informacao)
        elif(msg[0] == "I"):
            print(f"recebi I de {origem}")
            informacao += msg[1:]
            nao_recebidos.remove(origem)
            if len(nao_recebidos) == 0:
                print("todos vizinhos responderam, enviando token\ninformacao:", informacao)
    if estado == Estado.OCIOSO:
        if(msg[0] == "R"):
            print(f"recebi R de {origem}")
            entrada = origem
            nao_recebidos = Nx[:]
            nao_recebidos.remove(origem)
            iniciador = False
            estado = Estado.AGUARDANDO
            for vizinho in nao_recebidos:
                envia("R", vizinho)
    elif estado == Estado.AGUARDANDO:
        if(msg[0] == "R"):
            nao_recebidos.remove(origem)
            if len(nao_recebidos) == 0:
                envia("I" + informacao, entrada)
                estado = Estado.OK
        elif(msg[0] == "I"):
            nao_recebidos.remove(origem)
            informacao += msg[1:]
            if len(nao_recebidos) == 0:
                envia("I" + informacao, entrada)
                estado = Estado.OK



def callback(canal, metodo,  props, msg):
    m = msg.decode().split(":")
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
