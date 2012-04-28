# coding: utf-8
# modfied from https://github.com/phunt/zk-smoketest/blob/master/zkclient.py

import zookeeper, time, threading
from collections import namedtuple

DEFAULT_TIMEOUT = 30000
VERBOSE = True

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}

# Mapping of connection state values to human strings.
STATE_NAME_MAPPING = {
    zookeeper.ASSOCIATING_STATE: "associating",
    zookeeper.AUTH_FAILED_STATE: "auth-failed",
    zookeeper.CONNECTED_STATE: "connected",
    zookeeper.CONNECTING_STATE: "connecting",
    zookeeper.EXPIRED_SESSION_STATE: "expired",
}

# Mapping of event type to human string.
TYPE_NAME_MAPPING = {
    zookeeper.NOTWATCHING_EVENT: "not-watching",
    zookeeper.SESSION_EVENT: "session",
    zookeeper.CREATED_EVENT: "created",
    zookeeper.DELETED_EVENT: "deleted",
    zookeeper.CHANGED_EVENT: "changed",
    zookeeper.CHILD_EVENT: "child", 
}

class ZKClientError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ClientEvent(namedtuple("ClientEvent", 'type, connection_state, path')):
    """
    A client event is returned when a watch deferred fires. It denotes
    some event on the zookeeper client that the watch was requested on.
    """

    @property
    def type_name(self):
        return TYPE_NAME_MAPPING[self.type]

    @property
    def state_name(self):
        return STATE_NAME_MAPPING[self.connection_state]

    def __repr__(self):
        return  "<ClientEvent %s at %r state: %s>" % (
            self.type_name, self.path, self.state_name)


def watchmethod(func):
    def decorated(handle, atype, state, path):
        event = ClientEvent(atype, state, path)
        return func(event)
    return decorated

class ZKClient(object):
    def __init__(self, servers, timeout=DEFAULT_TIMEOUT):
        self.timeout = timeout
        self.connected = False
        self.conn_cv = threading.Condition( )
        self.handle = -1

        self.conn_cv.acquire()
        if VERBOSE: print("Connecting to %s" % (servers))
        start = time.time()
        self.handle = zookeeper.init(servers, self.connection_watcher, timeout)
        self.conn_cv.wait(timeout/1000)
        self.conn_cv.release()

        if not self.connected:
            raise ZKClientError("Unable to connect to %s" % (servers))

        if VERBOSE:
            print("Connected in %d ms, handle is %d"
                  % (int((time.time() - start) * 1000), self.handle))

    def connection_watcher(self, h, type, state, path):
        self.handle = h
        self.conn_cv.acquire()
        self.connected = True
        self.conn_cv.notifyAll()
        self.conn_cv.release()

    def close(self):
        return zookeeper.close(self.handle)
    
    def create(self, path, data="", flags=0, acl=[ZOO_OPEN_ACL_UNSAFE]):
        start = time.time()
        result = zookeeper.create(self.handle, path, data, acl, flags)
        if VERBOSE:
            print("Node %s created in %d ms"
                  % (path, int((time.time() - start) * 1000)))
        return result

    def delete(self, path, version=-1):
        start = time.time()
        result = zookeeper.delete(self.handle, path, version)
        if VERBOSE:
            print("Node %s deleted in %d ms"
                  % (path, int((time.time() - start) * 1000)))
        return result

    def get(self, path, watcher=None):
        return zookeeper.get(self.handle, path, watcher)

    def exists(self, path, watcher=None):
        return zookeeper.exists(self.handle, path, watcher)

    def set(self, path, data="", version=-1):
        return zookeeper.set(self.handle, path, data, version)

    def set2(self, path, data="", version=-1):
        return zookeeper.set2(self.handle, path, data, version)


    def get_children(self, path, watcher=None):
        return zookeeper.get_children(self.handle, path, watcher)

    def async(self, path = "/"):
        return zookeeper.async(self.handle, path)

    def acreate(self, path, callback, data="", flags=0, acl=[ZOO_OPEN_ACL_UNSAFE]):
        result = zookeeper.acreate(self.handle, path, data, acl, flags, callback)
        return result

    def adelete(self, path, callback, version=-1):
        return zookeeper.adelete(self.handle, path, version, callback)

    def aget(self, path, callback, watcher=None):
        return zookeeper.aget(self.handle, path, watcher, callback)

    def aexists(self, path, callback, watcher=None):
        return zookeeper.aexists(self.handle, path, watcher, callback)

    def aset(self, path, callback, data="", version=-1):
        return zookeeper.aset(self.handle, path, data, version, callback)

watch_count = 0

"""Callable watcher that counts the number of notifications"""
class CountingWatcher(object):
    def __init__(self):
        self.count = 0
        global watch_count
        self.id = watch_count
        watch_count += 1

    def waitForExpected(self, count, maxwait):
        """Wait up to maxwait for the specified count,
        return the count whether or not maxwait reached.

        Arguments:
        - `count`: expected count
        - `maxwait`: max milliseconds to wait
        """
        waited = 0
        while (waited < maxwait):
            if self.count >= count:
                return self.count
            time.sleep(1.0);
            waited += 1000
        return self.count

    def __call__(self, handle, typ, state, path):
        self.count += 1
        if VERBOSE:
            print("handle %d got watch for %s in watcher %d, count %d" %
                  (handle, path, self.id, self.count))

"""Callable watcher that counts the number of notifications
and verifies that the paths are sequential"""
class SequentialCountingWatcher(CountingWatcher):
    def __init__(self, child_path):
        CountingWatcher.__init__(self)
        self.child_path = child_path

    def __call__(self, handle, typ, state, path):
        if not self.child_path(self.count) == path:
            raise ZKClientError("handle %d invalid path order %s" % (handle, path))
        CountingWatcher.__call__(self, handle, typ, state, path)

class Callback(object):
    def __init__(self):
        self.cv = threading.Condition()
        self.callback_flag = False
        self.rc = -1

    def callback(self, handle, rc, handler):
        self.cv.acquire()
        self.callback_flag = True
        self.handle = handle
        self.rc = rc
        handler()
        self.cv.notify()
        self.cv.release()

    def waitForSuccess(self):
        while not self.callback_flag:
            self.cv.wait()
        self.cv.release()

        if not self.callback_flag == True:
            raise ZKClientError("asynchronous operation timed out on handle %d" %
                             (self.handle))
        if not self.rc == zookeeper.OK:
            raise ZKClientError(
                "asynchronous operation failed on handle %d with rc %d" %
                (self.handle, self.rc))


class GetCallback(Callback):
    def __init__(self):
        Callback.__init__(self)

    def __call__(self, handle, rc, value, stat):
        def handler():
            self.value = value
            self.stat = stat
        self.callback(handle, rc, handler)

class SetCallback(Callback):
    def __init__(self):
        Callback.__init__(self)

    def __call__(self, handle, rc, stat):
        def handler():
            self.stat = stat
        self.callback(handle, rc, handler)

class ExistsCallback(SetCallback):
    pass

class CreateCallback(Callback):
    def __init__(self):
        Callback.__init__(self)

    def __call__(self, handle, rc, path):
        def handler():
            self.path = path
        self.callback(handle, rc, handler)

class DeleteCallback(Callback):
    def __init__(self):
        Callback.__init__(self)

    def __call__(self, handle, rc):
        def handler():
            pass
        self.callback(handle, rc, handler)
