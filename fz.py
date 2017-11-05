import websocket
import thread
import time

def on_message(ws, message):
    print('[RECV]', message)

def on_error(ws, error):
    print('[ERR]', error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('7:publish,4:test,5:world,')
    # ws.send('9:subscribe,4:test,')


if __name__ == "__main__":
    # websocket.enableTrace(True)
    ws = websocket.WebSocketApp("ws://localhost:8080/ws",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
