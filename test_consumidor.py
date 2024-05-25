import pytest
from cosumidor import Singleton, Observer, ChainHandler

def test_singleton():
    # Test Singleton 
    instance1 = Singleton.get_instance()
    instance2 = Singleton.get_instance()

    assert instance1 is instance2



def test_observer():
    # Test Observer 
    observer = Observer()

    with pytest.raises(TypeError):
        observer.update()  # Deberia de sacar que es un metodo abstracto



class PruebaHand(ChainHandler):
    def __init__(self):
        super().__init__()
        self.handled_data = []

    def handle(self, data):
        self.handled_data.append(data)


def test_chain_handler_single():
    handler1 = PruebaHand()

    handler1.handle("Data1")
    assert handler1.handled_data == ["Data1"]


def test_chain_handler_multiple():
    handler1 = PruebaHand()
    handler2 = PruebaHand()
    handler3 = PruebaHand()

    handler1.set_next(handler2)
    handler2.set_next(handler3)

    handler1.handle("Data1")

    assert handler1.handled_data == ["Data1"]
    assert handler2.handled_data == ["Data1"]
    assert handler3.handled_data == ["Data1"]


def test_chain_handler_with_observer():
    class PruebaObserver(Observer):
        def __init__(self):
            self.observed_data = []

        def update(self, data):
            self.observed_data.append(data)

    handler1 = PruebaHand()
    observer1 = PruebaObserver()
    observer2 = PruebaObserver()

    handler1.add_observer(observer1)
    handler1.add_observer(observer2)

    handler1.handle("Data1")

    assert handler1.handled_data == ["Data1"]
    assert observer1.observed_data == ["Data1"]
    assert observer2.observed_data == ["Data1"]


def test_chain_handler_with_multiple_observers():
    class MockObserver(Observer):
        def __init__(self):
            self.observed_data = []

        def update(self, data):
            self.observed_data.append(data)

    handler1 = PruebaHand()
    observer1 = MockObserver()
    observer2 = MockObserver()

    handler1.add_observer(observer1)
    handler1.add_observer(observer2)

    handler1.handle("Data1")

    assert handler1.handled_data == ["Data1"]
    assert observer1.observed_data == ["Data1"]
    assert observer2.observed_data == ["Data1"]

    handler1.handle("Data2")

    assert handler1.handled_data == ["Data1", "Data2"]
    assert observer1.observed_data == ["Data1", "Data2"]
    assert observer2.observed_data == ["Data1", "Data2"]


if __name__ == "__main__":
    pytest.main()
