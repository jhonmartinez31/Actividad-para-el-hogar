"""
Productor de Kafka - Eventos de Pedidos
Publica eventos asíncronos de tipo 'pedido creado' al topic 'pedidos_topic'.
Utiliza serialización JSON y asignación de claves para distribución entre particiones.
"""

import datetime
import json
import random
import time
import uuid
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuración del broker y topic
BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC_NAME = "pedidos_topic"


def create_producer():
    """
    Inicializa y retorna una instancia configurada de KafkaProducer.
    """
    print(f"[Producer] Conectando al broker Kafka en {BOOTSTRAP_SERVERS}...")
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Serializar la clave como bytes UTF-8
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # Serializar el payload JSON a bytes UTF-8
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        # Garantía de entrega con confirmación de todos los líderes
        acks="all",
        retries=3
    )


def generate_mock_order(index: int) -> dict:
    """
    Genera un evento de negocio simulado de 'Pedido Creado'.
    """
    products = [
        {"item": "Teclado Mecánico RGB", "price": 85.00},
        {"item": "Mouse Inalámbrico Ergonómico", "price": 45.50},
        {"item": "Monitor 27'' 4K UHD", "price": 320.00},
        {"item": "Auriculares Cancelación de Ruido", "price": 110.00},
        {"item": "Silla Ergonómica Pro", "price": 250.00},
        {"item": "Hub USB-C 8 en 1", "price": 35.00}
    ]

    selected_product = random.choice(products)
    quantity = random.randint(1, 3)
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    customer_id = f"CUST-00{random.randint(1, 8)}"

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "order_id": order_id,
        "customer_id": customer_id,
        "item": selected_product["item"],
        "quantity": quantity,
        "unit_price": selected_product["price"],
        "total_amount": round(selected_product["price"] * quantity, 2),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }


def main():
    try:
        producer = create_producer()
    except KafkaError as e:
        print(f"\n[ERROR] No se pudo conectar a Kafka: {e}")
        print("Asegúrate de haber levantado el contenedor con: docker compose up -d\n")
        return

    print("=" * 65)
    print(f"  Kafka Producer Iniciado - Enviando eventos a '{TOPIC_NAME}'")
    print("=" * 65)

    # Cantidad de eventos a simular
    num_events = 8

    for i in range(1, num_events + 1):
        order_event = generate_mock_order(i)
        # Usamos el customer_id como key para que Kafka distribuya
        # los mensajes en las distintas particiones mediante murmur2 hash
        partition_key = order_event["customer_id"]

        print(f"\n[{i}/{num_events}] Publicando evento de pedido: {order_event['order_id']}")
        print(f"      Key (Customer): {partition_key}")
        print(f"      Item          : {order_event['item']} x{order_event['quantity']}")
        print(f"      Total         : ${order_event['total_amount']}")

        # Envío asíncrono retornando un Future de metadatos
        future = producer.send(
            topic=TOPIC_NAME,
            key=partition_key,
            value=order_event
        )

        try:
            # Esperar confirmación de persistencia en el broker
            record_metadata = future.get(timeout=10)
            print(f"      -> [CONFIRMADO BROKER] Topic: {record_metadata.topic} | "
                  f"Partición: {record_metadata.partition} | Offset: {record_metadata.offset}")
        except KafkaError as err:
            print(f"      -> [ERROR al enviar]: {err}")

        # Pequeño intervalo de emisión
        time.sleep(0.8)

    # Asegurar que todos los mensajes en búfer hayan sido enviados
    producer.flush()
    producer.close()

    print("\n" + "=" * 65)
    print("  Todos los eventos fueron publicados satisfactoriamente en Kafka.")
    print("=" * 65)


if __name__ == "__main__":
    main()
