#!/usr/bin/env python3
import sys, time
import json
import socket
import sys

from daemon import Daemon
from twisted.internet.protocol import Protocol, Factory
from twisted.internet import reactor

PORT = 16000
HOST = 'localhost'

class Counter:
    def __init__(self):
        self.count = 0

    def __repr__(self):
        return str(self.count)

    def __str__(self):
        return str(self.count)

    def incr(self):
        self.count += 1

    def decr(self):
        self.count -= 1

counter = Counter()

def get_name():
    import __main__
    try:
        location = __main__.__file__
    except:
        location = 'interpreter'
    name = '/'.join([location, str(counter)])
    counter.incr()
    return name

class Queue:
    def __init__(self, messages=list()):
        self.messages = messages

    def __len__(self):
        return len(self.messages)

    def __repr__(self):
        '''readable representation of the queue'''
        return "<Queue: {} messages>".format(len(self))

    def add(self, messages):
        '''add messages to the queue'''
        if type(messages) == list:
            self.messages = messages + self.messages
        else:
            self.messages = [messages] + self.messages

    def pop(self, number=1):
        '''pop items from the queue'''
        if number == 1:
            return self.messages.pop()
        else:
            ret = self.messages[number:]
            self.messages = self.messages[:number]
            return ret


class Worker(Queue):
    def __init__(self, broker, messages=list(), actions=dict()):
        self.broker = broker
        self.messages = messages
        self.actions = actions


class Broker():
    def __init__(self, host=HOST, port=PORT, subscriptions=dict()):
        self.host = host
        self.port = port
        self.subscriptions = subscriptions
        self.consumers = dict()

    def subscribe(self, producers, consumer):
        if type(producers) == list:
            for producer in producers:
                if self.subscriptions.get(producer, False):
                    self.subscriptions[producer] = [consumer]
                else:
                    self.subscriptions[producer] += consumer
        else:
                if self.subscriptions.get(producers, False):
                    self.subscriptions[producers] += [consumer]
                else:
                    self.subscriptions[producers] = [consumer]

class Consumer(Worker):
    def __init__(self, name=None, broker=None, messages=list(), actions=dict()):
        if not name:
            name = get_name()
        self.messages = messages
        self.broker = broker
        self.name = name
        # Create a TCP/IP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect the socket to the port where the server is listening
        self.server_address = ('localhost', 16000)
        print('connecting to %s port %s' % self.server_address, file=sys.stderr)
        self.sock.connect(self.server_address)
        try:
            message = b'consumer: %s' % name.encode('utf')
            self.sock.sendall(message)
            # Look for the response
            data = self.sock.recv(4096)

        except:
            pass

    def subscribe(self, publisher):
        self.sock.sendall(b'subscribe:' + publisher.encode('utf') + b':' +
                self.name.encode('utf'))
        data = self.sock.recv(4096)
        print('received "%s"' % data, file=sys.stderr)

    def subscriptions(self):
        self.sock.sendall(b'subscriptions')
        data = self.sock.recv(4096)
        return json.loads(data.decode('utf'))

def left_pad(word, l):
    return b''.join([b'#' for i in range(l-len(word))])+word

class Producer(Worker):
    def __init__(self, consumers=list(), brokers=None, messages=list(),
                 actions=dict()):
        '''Messaging queue with consumers'''
        self.messages = messages
        if type(consumers) == list:
            self.consumers = consumers
        else:
            self.consumers = [consumers]

    def __repr__(self):
        rep = "<Producer: {} consumers, {}  messages>"
        return rep.format(len(self.consumers), len(self.messages))

    def publish(self, messages):
        for consumer in self.consumers:
            self.send(consumer, messages)

    @staticmethod
    def send(consumer, messages):
        pass

### Protocol Implementation

class BrokerHandler(Protocol):
    broker = Broker()
    def dataReceived(self, data):
        if data[:len(b'subscribe:')] == b'subscribe:':
            #connection.sendall(data)
            key, value = map(lambda x: x.decode('utf'),
                    data[len(b'subscribe:'):].split(b':'))
            self.broker.subscribe(key, value)
        elif data[:len(b'subscriptions')] == b'subscriptions':
            data = self.get_subscriptions()
        elif data[:len(b'consumer')] == b'consumer':
            data = data[len(b'consumer: '):]
            #self._peer = self.transport.getPeer()
            self.broker.consumers[data] = self.transport.getPeer()
        self.transport.write(data)

    def get_subscriptions(self, consumer='all', producer='all'):
        return json.dumps(self.broker.subscriptions).encode('utf')


class ConsumerHandler(Protocol, Consumer):
    def __init__(self):
        Consumer.__init__(self)

    def dataReceived(self, data):
        # received a message
        if data[:len(b'message:')] == b'message:':
            #connection.sendall(data)
            message = json.loads(data[len(b'message:'):].decode('utf'))
            self.add(message)


class BaseDaemon(Daemon):
    def __init__(self, handler, daemon = True):
        if daemon:
            Daemon.__init__(self)
        self.handler = handler
        self.broker = Broker()

    def run(self, port=PORT, host=HOST):
        f = Factory()
        f.protocol = self.handler
        reactor.listenTCP(port, f)
        reactor.run()

    def get_subscriptions(self, consumer='all', producer='all'):
        return [self.broker.subscriptions, self.broker.consumers]


def BrokerDaemon():
    return BaseDaemon(BrokerHandler)


if __name__ == "__main__":
    daemon = BrokerDaemon()
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print("Unknown command")
            sys.exit(2)
            sys.exit(0)
    else:
        print("usage: %s start|stop|restart" % sys.argv[0])
        sys.exit(2)
