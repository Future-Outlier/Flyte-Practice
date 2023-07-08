def star(func):
    def inner(*args, **kwargs):
        print("*" * 15)
        func(*args, **kwargs)
        print("*" * 15)
    return inner


def percent(func):
    def inner(*args, **kwargs):
        print("%" * 15)
        func(*args, **kwargs)
        print("%" * 15)
    return inner

'''
def printer(msg):
    print(msg)
printer = star(percent(printer))
''' 

@star
@percent
def printer(msg):
    print(msg)

printer("Hello")

print("=" * 50)

@percent
@star
def printer(msg):
    print(msg)

printer("Hello")