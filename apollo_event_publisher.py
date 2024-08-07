import redis
import json
import uuid


class ApolloEventPublisher:
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db=0,
    ):
        self.redis_client = redis.StrictRedis(
            host=redis_host, port=redis_port, db=redis_db
        )
        self.event_handler_map = {
            "AuctionEndedEvent": self.auction_ended,
            "AuctionExtendEvent": self.auction_extended,
            "AuctionKilledEvent": self.auction_killed,
            "AuctionStartedEvent": self.auction_started,
            "BidPlacedEvent": self.notify_bid_update,
            "LotExtendedEvent": self.lot_extended,
            "LotWithdrawnEvent": self.lot_withdrawn,
            "AuctionLotsEndedEvent": self.lots_ended,
            "UserLotWon": self.lot_won_by_user,
        }

    def publish_to_redis(self, channel, message):
        self.redis_client.publish(channel, json.dumps(message))
        print(channel, json.dumps(message))

    @staticmethod
    def generate_event_id():
        return str(uuid.uuid4())

    def auction_finalized(self, data):
        event_id = self.generate_event_id()
        message = {
            "auctionUuid": data["auction_id"],
            "auctionName": data.get("published_name"),
            "type": "auction_finalized",
            "id": event_id,
        }
        channel = "user"
        self.publish_to_redis(channel, message)

    def auction_ended(self, data):
        event_id = self.generate_event_id()
        message = {
            "auctionUuid": data["auction_id"],
            "publishedName": data.get("published_name"),
            "type": "auction_ended",
            "id": event_id,
        }
        channel = f"auction.{data['auction_id']}"
        self.publish_to_redis(channel, message)
        self.auction_finalized(data)

    def auction_extended(self, data):
        event_id = self.generate_event_id()
        message = {
            "auctionUuid": data["auction_id"],
            "type": "auction_extended",
            "id": event_id,
        }
        channel = f"auction.{data['auction_id']}"
        self.publish_to_redis(channel, message)

    def auction_killed(self, data):
        event_id = self.generate_event_id()
        message = {
            "auctionUuid": data["auction_id"],
            "type": "auction_killed",
            "id": event_id,
        }
        channel = f"auction.{data['auction_id']}"
        self.publish_to_redis(channel, message)

    def auction_started(self, data):
        event_id = self.generate_event_id()
        message = {
            "auctionUuid": data["auction_id"],
            "publishedName": data.get("published_name"),
            "type": "auction_started",
            "id": event_id,
        }
        channel = f"auction.{data['auction_id']}"
        self.publish_to_redis(channel, message)

    def notify_bid_update(self, data):
        event_id = self.generate_event_id()
        current_bid = data["current_bid"]
        message = {
            "auctionUuid": data["auction_id"],
            "lotNumber": data["lot_number"],
            "type": "notify_bid_update",
            "currentBid": {
                "amount": current_bid["amount"]["value"],
                "userId": current_bid["user_id"],
                "currency": current_bid["amount"]["currency"],
                "createdAt": current_bid["created_at"],
            },
            "id": event_id,
        }
        channel = f"auction.{data['auction_id']}.lot.{data['lot_number']}"
        self.publish_to_redis(channel, message)

        if (
            data.get("previous_bid")
            and data["previous_bid"]["user_id"] != current_bid["user_id"]
        ):
            self.out_bid(data)

    def out_bid(self, data):
        event_id = self.generate_event_id()
        previous_bid = data["previous_bid"]
        message = {
            "auctionUuid": data["auction_id"],
            "lotUuid": data["lot_id"],
            "lotName": data["lot_title"],
            "type": "out_bid",
            "id": event_id,
        }
        channel = f"user.{previous_bid['user_id']}"
        self.publish_to_redis(channel, message)

    def lot_extended(self, data):
        event_id = self.generate_event_id()
        message = {
            "auctionUuid": data["auction_id"],
            "lotNumber": data["lot_number"],
            "type": "lot_extended",
            "id": event_id,
        }
        channel = f"auction.{data['auction_id']}.lot.{data['lot_number']}"
        self.publish_to_redis(channel, message)

    def lot_withdrawn(self, data):
        event_id = self.generate_event_id()
        message = {
            "auctionUuid": data["auction_id"],
            "lotNumber": data["lot_number"],
            "type": "lot_withdrawn",
            "id": event_id,
        }
        channel = f"auction.{data['auction_id']}.lot.{data['lot_number']}"
        self.publish_to_redis(channel, message)

    def lots_ended(self, data):
        event_id = self.generate_event_id()
        message = {
            "auctionUuid": data["auction_id"],
            "type": "lots_ended",
            "id": event_id,
        }
        channel = f"auction.{data['auction_id']}"
        self.publish_to_redis(channel, message)

    def lot_won_by_user(self, data):
        event_id = self.generate_event_id()
        message = {
            "userEmail": data.get("email"),
            "lotNumber": data.get("number"),
            "lotTitle": data.get("title"),
            "currency": data.get("currency"),
            "bidAmount": data.get("amount"),
            "id": event_id,
        }
        channel = "users.LotWon"
        self.publish_to_redis(channel, message)

    def map_and_publish_event(self, event_type, event_data):
        if event_type in self.event_handler_map:
            self.event_handler_map[event_type](json.loads(event_data))
        else:
            return
