"""
Servidor gRPC - StoreService
Implementa la comunicación síncrona mediante RPC (Remote Procedure Call)
utilizando Protocol Buffers sobre HTTP/2.
"""

from concurrent import futures
import datetime
import time
import uuid
import grpc

from proto import store_pb2
from proto import store_pb2_grpc


class StoreServiceServicer(store_pb2_grpc.StoreServiceServicer):
    """
    Implementación del servicio StoreService definido en store.proto.
    Maneja llamadas Unary (CreateOrder) y Server-Streaming (StreamOrderUpdates).
    """

    def CreateOrder(self, request, context):
        """
        Método Unary (1 petición -> 1 respuesta).
        Procesa la creación síncrona de un pedido y retorna su confirmación inmediata.
        """
        # Generar un identificador único para el pedido
        generated_order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        timestamp_now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        total_price = request.quantity * request.price

        print("\n" + "=" * 55)
        print(f"[gRPC Server] [Unary: CreateOrder] Petición recibida:")
        print(f"  - ID Generado  : {generated_order_id}")
        print(f"  - Cliente ID   : {request.customer_id}")
        print(f"  - Producto     : {request.item_name}")
        print(f"  - Cantidad     : {request.quantity}")
        print(f"  - Precio Unit. : ${request.price:.2f}")
        print(f"  - Total Calcul.: ${total_price:.2f}")
        print("=" * 55)

        # Construir y retornar la respuesta protobuf fuertemente tipada
        return store_pb2.OrderResponse(
            order_id=generated_order_id,
            status="CREATED",
            message=f"Pedido '{request.item_name}' registrado exitosamente para el cliente '{request.customer_id}'.",
            timestamp=timestamp_now,
        )

    def StreamOrderUpdates(self, request, context):
        """
        Método Server-Streaming (1 petición -> Stream continuo de respuestas).
        Transmite una serie de eventos de estado para una orden específica a lo largo del tiempo.
        """
        order_id = request.order_id
        print(f"\n[gRPC Server] [Streaming: StreamOrderUpdates] Iniciando stream para Orden ID: {order_id}")

        # Flujo de ciclo de vida del pedido simulado
        lifecycle_events = [
            ("ORDER_RECEIVED", "Pedido recibido por el sistema y en cola de validación."),
            ("INVENTORY_RESERVED", "Stock reservado exitosamente en bodega central."),
            ("PAYMENT_CONFIRMED", "Transacción de pago aprobada por la pasarela."),
            ("PACKAGING", "Empaque de productos finalizado y listo para despacho."),
            ("IN_TRANSIT", "En ruta hacia el domicilio con guía de seguimiento."),
            ("DELIVERED", "Pedido entregado exitosamente al cliente.")
        ]

        for status_code, detail in lifecycle_events:
            # Verificar si el cliente canceló la conexión prematuramente
            if not context.is_active():
                print(f"[gRPC Server] Cliente desconectado. Cancelando stream para {order_id}.")
                break

            current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
            status_update = store_pb2.OrderStatus(
                order_id=order_id,
                status=status_code,
                detail=detail,
                timestamp=current_time,
            )

            print(f"  >> [Emitiendo Estado] {order_id} -> [{status_code}]: {detail}")
            yield status_update

            # Simular retraso entre eventos de negocio
            time.sleep(1.2)

        print(f"[gRPC Server] Stream completado para Orden ID: {order_id}\n")


def serve():
    """
    Configura e inicia el servidor gRPC en el puerto 50051.
    """
    # Pool de hilos para concurrencia en la atención de llamadas RPC
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Registrar la implementación del servicio en el servidor
    store_pb2_grpc.add_StoreServiceServicer_to_server(StoreServiceServicer(), server)

    # Enlazar en el puerto 50051 en todas las interfaces de red
    port = "50051"
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"============================================================")
    print(f"  Servidor gRPC StoreService activo y escuchando en el puerto {port}")
    print(f"  Presiona Ctrl+C para detener el servidor")
    print(f"============================================================")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nDeteniendo servidor gRPC de manera ordenada (Graceful Shutdown)...")
        server.stop(grace=3)
        print("Servidor detenido.")


if __name__ == "__main__":
    serve()
