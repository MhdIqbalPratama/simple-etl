from services.consumer_services import ConsumerService

konsum = ConsumerService()

print(konsum.consume_messages(100))
