import functools


def check_flag(func):
    @functools.wraps(func)
    def wrapped(self, flag, **kwargs):
        if flag is True:
            return func(self, flag, **kwargs)
        else:
            return "a"
    return wrapped


class A:
    def printf(self, flag):
        print "a"


class B(A):
    @check_flag
    def printf(self, flag):
        super(B, self)
        print "b"


b = B()
b.printf(False)
