from kodo.worker.loader import Loader, publish
from kodo.flow.simple import flow

def loader1():
    loader = Loader()
    loader.add_flow(entry_point=flow)
    return loader

def loader2():
    loader = Loader()
    loader.add_flow(
        entry_point=flow
    )
    return loader

def loader3():
    loader = Loader()
    loader.add_flow(
        entry_point=flow,
        name="mytest2",
        description="This is a testing flow",
        url="/mytest2",
        author="michi.rau@gmail.com"
    )
    return loader
