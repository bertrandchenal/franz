#!/usr/bin/env python
from time import time
import argparse
import sys

import websocket
from websocket import _logging, WebSocketTimeoutException

# websocket.enableTrace(False)
# _logging._logger.setLevel('ERROR')

ts2dt = lambda x: x if not x else datetime.fromtimestamp(x)
dt2ts = lambda x: x if not x else mktime(x.timetuple())


def pack(*items):
    '''
    Pack items as a netstring
    '''
    return ''.join('%s:%s,' % (len(x) , x) for x in items)


def unpack(data):
    '''
    Unpack netstring
    '''
    while data:
        head, tail = data.split(b':', 1)
        length = int(head)
        item = tail[:length]
        assert tail[length:length+1] == b','
        yield item
        data = tail[length + 1:]


def mktime(time_str):
    if isinstance(time_str, datetime):
        return time_str
    elif isinstance(time_str, date):
        return datetime(time_str.year, time_str.month, time_str.day)

    candidates = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            pass
    raise ValueError('Unable to parse "%s" as datetime' % time_str)


def pub(ws, tube, tags=None, lines=None):
    if not lines:
        lines = [sys.stdin.read().strip()]
    for line in lines:
        msg = pack('pub', tube, line, *tags)
        ws.send(msg)
        ws.recv()


def send_sub(ws, tube, offset=None, timestamp=None, tags=None):
    offset = str(offset or 0)
    if timestamp:
        timestamp = str(dt2ts(timestamp))
    else:
        timestamp = '0'
    tags = tags or []
    msg = pack('sub', tube, offset, timestamp, *tags)
    ws.send(msg)


def sub(ws, tube, offset=None, timestamp=None, tags=None, follow=False):
    offset = offset or 0
    send_sub(ws, tube, offset, timestamp, tags)
    while True:
        try:
            payload = ws.recv()
        except WebSocketTimeoutException:
            if follow:
                send_sub(ws, tube, offset, timestamp, tags)
            else:
                break
        else:
            msg, offset = unpack(payload)
            offset = int(offset)
            yield msg.decode('utf-8')


def main(cli):
    ws = websocket.create_connection("ws://localhost:9090/ws")
    ws.settimeout(1)
    action, tube, *args = cli.action
    if action == 'pub':
        pub(ws, tube, tags=cli.tags, lines=args)
    elif action == 'sub':
        chunks = sub(ws, tube,
                     offset=cli.offset,
                     timestamp=cli.timestamp,
                     tags=cli.tags,
                     follow=cli.follow)
        for chunk in chunks:
            print(chunk)
    elif action == 'bench':
        bench(ws)
    else:
        print('Action "%s" not supported' % action)

    ws.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', nargs='*',
                        help='pub <tube> <msg> |sub <tube>')
    parser.add_argument('-f', '--follow', action='store_true',
                        help='Wait for new content when end of tube is reached',
                        )
    parser.add_argument('-t', '--tags', action='append', default=[],
                        help='Specify one or several tags',
                        )
    parser.add_argument('-T', '--timestamp', type=mktime,
                        help='Filter by timestamp',
                        )
    parser.add_argument('-o', '--offset', type=int,
                        help='Filter by offset',
                        )
    cli = parser.parse_args()
    if not cli.action:
        parser.print_help()
    else:
        main(cli)
