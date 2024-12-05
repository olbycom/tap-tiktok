from logging.handlers import SocketHandler


class FluentBitHandler(SocketHandler):
    def emit(self, record):
        try:
            self.send((self.format(record)).encode())
        except Exception:
            self.handleError(record)
