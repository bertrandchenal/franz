from time import time
import argparse
import sys

import websocket
from websocket import _logging, WebSocketTimeoutException

websocket.enableTrace(False)
_logging._logger.setLevel('ERROR')


def pub(ws, tube, *lines):
    if not lines:
        lines = (l.strip() for l in sys.stdin)
    for line in lines:
        ws.send('7:publish,%s:%s,%s:%s,' % (
            len(tube), tube, len(line), line))
    ws.recv()


def send_sub(ws, tube, offset):
    msg = '9:subscribe,%s:%s,' % (len(tube), tube)
    if offset:
        offset = str(offset)
        msg += '%s:%s,' % (len(offset), offset)
    ws.send(msg)


def sub(ws, tube, offset=0, follow=False):
    send_sub(ws, tube, offset)
    if not follow:
        ws.settimeout(1)
    while True:
        try:
            data = ws.recv()
            offset += len(data)
        except WebSocketTimeoutException:
            break
        yield data


def bench(ws):
    payload = 'bench' * 1000
    start = time()
    for i in range(100):
        resp = pub(ws, 'bench', payload)
        assert resp == b'OK'
    print('PUB', time() - start)

    start = time()
    cnt = 0
    for msg in sub(ws, 'bench'):
        assert payload == msg.decode()
        cnt += 1
        if cnt == 100:
            break

    print('SUB', time() - start)


def main(cli):
    ws = websocket.create_connection("ws://localhost:9090/ws")
    action, *args = cli['action']
    if action == 'pub':
        pub(ws, *args)
    elif action == 'sub':
        for chunk in sub(ws, *args, follow=cli['follow']):
            print(chunk)
    elif action == 'bench':
        bench(ws)
    else:
        print('Action "%s" not supported' % action)

    ws.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', nargs='*', help='pub|sub')
    parser.add_argument('-f', '--follow', action='store_true',
                        help='Wait for new content when end of tube is reached',
                        )
    cli = parser.parse_args()

    main(vars(cli))
