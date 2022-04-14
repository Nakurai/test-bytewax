from dataclasses import dataclass
import json
from logging import warning
import sys
from typing import List

from bytewax import Dataflow, parse, spawn_cluster
from bytewax.inputs import fully_ordered

class Client:
  """
  This class contains all the information regarding our clients.
  """
  _user_id: str
  _name: str
  _orders: List

  def __init__(self, user_id):
    self._user_id = user_id
    self._name = ""
    self._orders = []

  def set_name(self, name:str):
    """
    Update the client's name
    """
    if name is not None:
      self._name = name

  def add_order(self, new_order:dict):
    """
    Add an order to the client's history
    """
    self._orders.append(new_order)

  def create_stream_event(self):
    """
    Create a new customer event containing all the expected information
    this specific client
    """
    return {
      "user_id":self._user_id,
      "name":self._name,
      "event":{"type":"customer"},
      "orders": self._orders
    }

def kafka_input():
    """Simulate a stream of kafka input from a file."""
    with open("events.json") as file:
        for msg in file:
          yield json.loads(msg)

            
def input_builder(worker_index, worker_count):
    return fully_ordered(kafka_input())

def output_builder(worker_index, worker_count):
  def output_handler(epoch_item):
    epoch, item = epoch_item
    print(item)
    return item
  return output_handler

def has_user_id(stream_input):
  """
  The state of this program depends on streams having a user id,
  this function helps filter those that don't.
  """
  user_id = stream_input.get("user_id")
  if user_id is None:
    warning(f"No user id detected")
    return False
  else:
    return True

def map_user_id(stream_input):
  return (stream_input.get('user_id'), stream_input)

def build_client_state(user_id):
  """
  Given a user_id, this will create a new Client instance and keep
  it in the state
  """
  return Client(user_id)

def update_client_state(client, stream_input):
  """
  We are handling two types of event related to a client:
   - customer: which updates their personal information
   - order: which add an order in their purchase history
  
  Unkown types of order do not update the client's state, but still
  return a joined stream.
  """

  event_type = stream_input.get("event", {}).get("type")
  if event_type == "customer":
    client.set_name(stream_input.get("name"))
  elif event_type == "order":
    client.add_order(stream_input)
  else:
    # This should probably send a message to someone so they can investigate
    # It's unclear if we should still trigger an event in this case. If not, we could
    # send a specific key downstream and use the filter operator.
    warning(f"Uknown event type: {event_type} (user_id: {stream_input.get('user_id')})")

  joined_stream = client.create_stream_event()

  return client, joined_stream

def remove_map_key(stream_input):
  key, event = stream_input
  return event

flow = Dataflow()
flow.filter(has_user_id)
flow.map(map_user_id)
flow.stateful_map(build_client_state, update_client_state)
flow.map(remove_map_key)
flow.capture()

if __name__ == "__main__":
  spawn_cluster(flow, input_builder, output_builder, **parse.cluster_args())