#!/usr/bin/env python3
import sys, time
import socket
import sys
import sys, os, time, atexit
from signal import SIGTERM

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
    return name.encode('utf8')

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
                self.subscriptions[producer] = consumer
        else:
            self.subscriptions[producers] = consumer


class Consumer(Worker):
    def __init__(self, name=get_name(), broker=None, messages=list(), actions=dict()):
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
            message = b'consumer: %s' % name
            self.sock.sendall(left_pad(message, 64))
            data = self.sock.recv(64)
        except:
            pass

    def subscribe(self, publisher):
        data = left_pad(b'subscribe %s' % publisher+b':'+self.name, 64)
        self.sock.sendall(data)
        data = self.sock.recv(64)
        print('received "%s"' % data.rstrip(b'#'), file=sys.stderr)

    def subscriptions(self):
        self.sock.sendall(b'subscriptions')
        data = self.sock.recv(256)
        return data.rstrip(b'#')
        #finally:
        #    print('closing socket', file=sys.stderr)
        #    sock.close()

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

class Daemon:
        """
        A generic daemon class.
        Usage: subclass the Daemon class and override the run() method
        """
        def __init__(self, pidfile='/tmp/hermesd.pid', stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
                self.stdin = stdin
                self.stdout = stdout
                self.stderr = stderr
                self.pidfile = pidfile

        def daemonize(self):
                """
                do the UNIX double-fork magic, see Stevens' "Advanced
                Programming in the UNIX Environment" for details (ISBN 0201563177)
                http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
                """
                try:
                        pid = os.fork()
                        if pid > 0:
                                # exit first parent
                                sys.exit(0)
                except OSError as e:
                        sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
                        sys.exit(1)

                # decouple from parent environment
                os.chdir("/")
                os.setsid()
                os.umask(0)

                # do second fork
                try:
                        pid = os.fork()
                        if pid > 0:
                                # exit from second parent
                                sys.exit(0)
                except OSError as e:
                        sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
                        sys.exit(1)

                # redirect standard file descriptors
                sys.stdout.flush()
                sys.stderr.flush()
                si = open(self.stdin, 'r')
                so = open(self.stdout, 'a+')
                se = open(self.stderr, 'ab+', 0)
                os.dup2(si.fileno(), sys.stdin.fileno())
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())

                # write pidfile
                atexit.register(self.delpid)
                pid = str(os.getpid())
                open(self.pidfile,'w+').write("%s\n" % pid)

        def delpid(self):
                os.remove(self.pidfile)

        def start(self, *args):
                """
                Start the daemon
                """
                # Check for a pidfile to see if the daemon already runs
                try:
                        pf = open(self.pidfile,'r')
                        pid = int(pf.read().strip())
                        pf.close()
                except IOError:
                        pid = None

                if pid:
                        message = "pidfile %s already exist. Daemon already running?\n"
                        sys.stderr.write(message % self.pidfile)
                        sys.exit(1)

                # Start the daemon
                self.daemonize()
                self.run(*args)

        def stop(self):
                """
                Stop the daemon
                """
                # Get the pid from the pidfile
                try:
                        pf = open(self.pidfile,'r')
                        pid = int(pf.read().strip())
                        pf.close()
                except IOError:
                        pid = None

                if not pid:
                        message = "pidfile %s does not exist. Daemon not running?\n"
                        sys.stderr.write(message % self.pidfile)
                        return # not an error in a restart

                # Try killing the daemon process
                try:
                        while 1:
                                os.kill(pid, SIGTERM)
                                time.sleep(0.1)
                except OSError as err:
                        err = str(err)
                        if err.find("No such process") > 0:
                                if os.path.exists(self.pidfile):
                                        os.remove(self.pidfile)
                        else:
                                print(str(err))
                                sys.exit(1)

        def restart(self):
                """
                Restart the daemon
                """
                self.stop()
                self.start()

        def run(self, *args):
                """
                You should override this method when you subclass Daemon. It will be called after the process has been
                daemonized by start() or restart().
                """


class BrokerDaemon(Daemon):
    def __init__(self, daemon = True):
        if daemon:
            Daemon.__init__(self)
        self.broker = Broker()

    def run(self, consumers=list(), port=PORT, host=HOST):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Bind the socket to the port
        server_address = (host, port)
        print('starting up on %s port %s' % server_address, file=sys.stderr)
        sock.bind(server_address)
        # Listen for incoming connections
        sock.listen(5)
        while True:
            # Wait for a connection
            print('waiting for a connection', file=sys.stderr)
            connection, client_address = sock.accept()

            def respond():
                try:
                    host, port = client_address
                    print('connection from', client_address, file=sys.stderr)

                    # Receive the data in small chunks and retransmit it
                    while True:
                        data = connection.recv(64)
                        data = data.lstrip(b'#')
                        if data[:len(b'subscribe')] == b'subscribe':
                            connection.sendall(left_pad(data, 64))
                            key, value = map(lambda x: x.strip(),
                                             data[len(b'subscribe'):].split(b':'))
                            self.broker.subscribe(key, value)
                        elif data[:len(b'subscriptions')] == b'subscriptions':
                            data = str(self.get_subscriptions()).encode('utf')
                            connection.sendall(left_pad(data,256))
                        elif data[:len(b'consumer')] == b'consumer':
                            data = data[len(b'consumer: '):]
                            self.broker.consumers[data] = host, port
                            connection.sendall(left_pad(data,64))
                        else:
                            import pdb; pdb.set_trace()
                            break
                except Exception as e:
                    open('log.txt','a').write(e)

            import threading
            t = threading.Thread(target=respond)
            t.daemon = True
            t.start()
            continue

    def get_subscriptions(self, consumer='all', producer='all'):
        return self.broker.subscriptions, self.broker.consumers


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
