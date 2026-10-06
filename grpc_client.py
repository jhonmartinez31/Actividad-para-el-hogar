"""
Cliente gRPC - StoreClient
Se conecta al servidor gRPC en localhost:50051 y consume tanto el método
Unary (CreateOrder) como el método Server-Streaming (StreamOrderUpdates),
mostrando evidencia formateada en consola.
"""

import sys
import grpc

from proto import store_pb2
from proto import store_pb2_grpc


def run():
    server_address = "localhost:50051"
    print("=" * 65)
    print(f"  Iniciando Cliente gRPC - Conectando a {server_address}")
    print("=" * 65)

    # Crear un canal gRPC (HTTP/2 multiplexado)
    with grpc.insecure_channel(server_address) as channel:
        stub = store_pb2_grpc.StoreServiceStub(channel)

        try:
            # -----------------------------------------------------------------
            # PARTE 1: Consumir método Unary (CreateOrder)
            # -----------------------------------------------------------------
            print("\n>>> [PASO 1] Invocando RPC Unary: CreateOrder...")
            request = store_pb2.OrderRequest(
                customer_id="USR-8942",
                item_name="Monitor UltraWide 34'' 144Hz",
                quantity=2,
                price=450.50
            )

            print(f"    Enviando OrderRequest:")
            print(f"      * Customer ID : {request.customer_id}")
            print(f"      * Producto    : {request.item_name}")
            print(f"      * Cantidad    : {request.quantity}")
            print(f"      * Precio      : ${request.price:.2f}")

            # Llamada síncrona bloqueante
            response = stub.CreateOrder(request)

            print("\n    [EVIDENCIA - Respuesta Unary Recibida de gRPC Server]:")
            print(f"      * ID de Orden   : {response.order_id}")
            print(f"      * Estado        : {response.status}")
            print(f"      * Mensaje       : {response.message}")
            print(f"      * Timestamp UTC : {response.timestamp}")

            created_order_id = response.order_id

            # -----------------------------------------------------------------
            # PARTE 2: Consumir método Server-Streaming (StreamOrderUpdates)
            # -----------------------------------------------------------------
            print("\n" + "-" * 65)
            print(f">>> [PASO 2] Invocando RPC Server-Streaming: StreamOrderUpdates...")
            print(f"    Solicitando flujo de seguimiento para la Orden: {created_order_id}")

            order_id_msg = store_pb2.OrderId(order_id=created_order_id)
            stream_iterator = stub.StreamOrderUpdates(order_id_msg)

            print("    [EVIDENCIA - Consumiendo Stream en Tiempo Real]:")
            step = 1
            for update in stream_iterator:
                print(f"      [Stream #{step}] [{update.timestamp}]")
                print(f"        -> Estado : {update.status}")
                print(f"        -> Detalle: {update.detail}")
                step += 1

            print("\n>>> Stream finalizado exitosamente por el servidor.")
            print("=" * 65)
            print("  Pruebas de cliente gRPC completadas con éxito.")
            print("=" * 65)

        except grpc.RpcError as rpc_error:
            print(f"\n[ERROR gRPC] Código: {rpc_error.code()}")
            print(f"[ERROR gRPC] Detalle: {rpc_error.details()}")
            print("\nAsegúrate de que el servidor gRPC esté en ejecución con: python grpc_server.py")
            sys.exit(1)


if __name__ == "__main__":
    run()
