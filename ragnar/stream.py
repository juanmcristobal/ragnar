import types
from functools import reduce
from collections.abc import Iterable
from itertools import tee, chain


# based on https://github.com/pytoolz/toolz/blob/master/toolz/functoolz.py
def func_cat(value, *forms):
    def evalform_front(value, form):
        if isinstance(value, Iterable) and callable(form):
            for obj in value:
                if obj is not None:
                    yield form(obj)

    return reduce(evalform_front, forms, value)


class Stream(object):
    def __init__(self, value, **kwargs):
        self.value = value

        # storage used for child classes, which will be used to store variables
        # between to recreate the stream. It is mandatory to store with the same name
        # as the variable in the child class.
        self.__set_store__(dict(kwargs))

        # working variables
        self.repeatable = kwargs.pop("repeatable", None)

        # Iterator config
        self.iter = None
        self.iter_repeatable = None

        # Func config
        self.__stack__ = []

    def __set_store__(self, parameter: dict):
        """
        Method that stores the initial settings for the Stream.
        :param parameter: A dictionary taken from the arguments **kwargs
        :return: None
        """
        self.__store__ = type("Store", (object,), parameter)

    def __regenerate_store__(self):
        for key in vars(self.__store__).keys():
            if not key.startswith("__"):
                setattr(self, key, getattr(self.__store__, key))

    def __build__(self):
        for stack in self.__stack__:
            if stack.type == "do":
                if stack.chain:
                    self.value = chain(*func_cat(self.value, stack.fuction))
                else:
                    self.value = func_cat(self.value, stack.fuction)
            if stack.type == "filter":
                self.value = filter(stack.fuction, self.value)

        if self.repeatable:
            self.iter, self.iter_repeatable = tee(iter(self.value))
        else:
            self.iter = iter(self.value)

    def __rebuild__(self):
        """
        This method will regenerate the iterator.
        """
        if self.repeatable:
            self.__regenerate_store__()
            self.iter, self.iter_repeatable = tee(self.iter_repeatable)

    def __iter__(self):
        if not self.iter:
            self.__build__()
        return self

    def __next__(self):
        try:
            return self.__show_next__()
        except StopIteration:
            self.__rebuild__()
            raise StopIteration

    def __show_next__(self):
        """
        This method allows you to treat the item before showing it.
        """
        item = self.iter.__next__()

        if isinstance(item, types.GeneratorType):
            return list(item)

        return item

    def do(self, func, chain=False):
        """
        This method adds a function to apply to the execution stack.
        """
        self.__stack__.append(
            type(
                "FunctionFactory",
                (object,),
                {"type": "do", "fuction": func, "chain": chain},
            )
        )
        return self

    def filter(self, func):
        """
        This method adds a filter to apply to the execution stack.
        """
        self.__stack__.append(
            type("FunctionFactory", (object,), {"type": "filter", "fuction": func})
        )
        return self
