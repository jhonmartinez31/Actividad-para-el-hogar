"""
Consumidor de Kafka - Microservicio de Inventario / Procesamiento
Se suscribe a 'pedidos_topic' utilizando el grupo de consumidores 'pedidos_group'.
Muestra explícitamente en consola la partición, offset y contenido de cada mensaje recibido,
evidenciando el reparto de carga entre consumidores del mismo grupo.
"""

import json
import sys
import uuid
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configuración del broker, topic y grupo de consumidores
BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC_NAME = "pedidos_topic"
GROUP_ID = "pedidos_group"


def create_consumer(client_id: str):
    """
    Inicializa y retorna una instancia configurada de KafkaConsumer.
    """
    print(f"[{client_id}] Conectando al broker Kafka en {BOOTSTRAP_SERVERS}...")
    print(f"[{client_id}] Suscribiéndose al topic '{TOPIC_NAME}' con Group ID: '{GROUP_ID}'...")

    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        client_id=client_id,
        # Iniciar lectura desde el primer offset si no hay commit previo
        auto_offset_reset="earliest",
        # Confirmación automática de offsets procesados
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        # Deserializadores de clave y valor JSON
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None
    )


def main():
    # Identificador de la instancia del consumidor (por argumento o aleatorio)
    consumer_instance_name = sys.argv[1] if len(sys.argv) > 1 else f"Consumidor-{uuid.uuid4().hex[:4].upper()}"

    print("=" * 70)
    print(f"  Kafka Consumer Iniciado: [{consumer_instance_name}]")
    print(f"  Grupo de Consumidores  : {GROUP_ID}")
    print(f"  Topic Asignado         : {TOPIC_NAME}")
    print("  Esperando asignación de particiones y nuevos mensajes...")
    print("  Presiona Ctrl+C para detener el consumidor")
    print("=" * 70)

    try:
        consumer = create_consumer(consumer_instance_name)
    except KafkaError as e:
        print(f"\n[ERROR] No se pudo conectar a Kafka: {e}")
        print("Verifica que el contenedor de Kafka esté activo con: docker compose up -d\n")
        return

    try:
        message_count = 0
        for message in consumer:
            message_count += 1
            payload = message.value

            print("\n" + "-" * 70)
            print(f"[{consumer_instance_name}] >> MENSAJE RECIBIDO #{message_count}:")
            # Requisito clave: Imprimir la partición de la que se lee el mensaje
            print(f"  * Partición Origen : [ PARTICIÓN {message.partition} ]")
            print(f"  * Offset           : {message.offset}")
            print(f"  * Partition Key    : {message.key}")
            print(f"  * Timestamp Broker : {message.timestamp}")
            print(f"  * Datos del Pedido :")
            if isinstance(payload, dict):
                print(f"      - Order ID     : {payload.get('order_id')}")
                print(f"      - Cliente      : {payload.get('customer_id')}")
                print(f"      - Producto     : {payload.get('item')} (x{payload.get('quantity')})")
                print(f"      - Total        : ${payload.get('total_amount')}")
                print(f"      - Tipo Evento  : {payload.get('event_type')}")
            else:
                print(f"      - Payload      : {payload}")
            print("-" * 70)

    except KeyboardInterrupt:
        print(f"\n[{consumer_instance_name}] Cerrando consumidor y liberando asignación de partición...")
    finally:
        consumer.close()
        print(f"[{consumer_instance_name}] Consumidor detenido.")


if __name__ == "__main__":
    main()
