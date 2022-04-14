from stream_join_template import Client, update_client_state

def test_update_name():
  client = Client('0001')
  stream_input = {"user_id": "0001", "name": "chris", "event": {"type": "customer"}}
  update_client_state(client, stream_input)
  assert client._name == "chris" 

def test_add_order():
  client = Client('0001')
  stream_input = {"user_id": "0001", "order_id": "1004", "event": {"type": "order"}}
  update_client_state(client, stream_input)
  assert len(client._orders) == 1 

def test_unknown_event_type():
  client = Client('0001')
  stream_input = {"user_id": "0001", "order_id": "1004", "event": {"type": "unknown"}}
  update_client_state(client, stream_input)
  assert len(client._orders) == 0 
  assert client._name == "" 