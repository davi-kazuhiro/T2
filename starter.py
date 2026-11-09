#!/usr/bin/env python3

# STARTER
#
# SDC - PUCPR
# Prof. Luiz Lima Jr.
#
# O "starter" envia um evento "externo"
# a um ou mais componentes do sistema.
# Quando o componente recebe uma mensagem
# do starter, ele passa a assumir o papel
# de iniciador.

from pika import BlockingConnection
from sys import argv

if len(argv) < 3:
    print(f"Uso: {argv[0]} <mensagem> <dest1> [<dest2> ...]")
    exit(1)

mensagem = argv[1]
destinos = argv[2:]

conexao = BlockingConnection()
canal = conexao.channel()

def envia(msg, dests):
    for d in dests:
        canal.basic_publish(exchange="",
                            routing_key=d,
                            body=f"STARTER:{msg}")
        print(f'Mensagem "{mensagem}" enviada para {d}')

for d in destinos:
    canal.queue_declare(queue=d, durable=True, auto_delete=True)

envia(mensagem, destinos)

conexao.close()

