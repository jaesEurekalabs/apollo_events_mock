from apollo_event_publisher import ApolloEventPublisher

REDIS_HOST = "localhost"
REDIS_PORT = 6379


def event_simulation():
    event_publisher = ApolloEventPublisher(redis_host=REDIS_HOST, redis_port=REDIS_PORT)

    for n in range(0, 100):
        lot_won = {
            "email": f"email_{n}@test.com",
            "number": n,
            "title": f"Lot {n}",
            "currency": "USD",
            "amount": n,
        }
        event_publisher.lot_won_by_user(lot_won)


event_simulation()
