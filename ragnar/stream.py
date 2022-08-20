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
    """This object brings together the advantages of generators and the functional programming paradigm.
    Stream is an object that allows accumulating actions to be applied to a dataset.

    :param repeatable: This parameter is used to enable repeating iterations if `'True'` allows to iterate as many
                       times as required
    :type repeatable: bool, optional

    """

    def __init__(self, value, **kwargs):
        self.value = value

        # storage used for child classes, which will be used to store variables
        # between to recreate the stream. It is mandatory to store with the same name
        # as the variable in the child class.
        self.__set_store__(dict(kwargs))

        # working variables
        self.repeatable = kwargs.pop("repeatable", False)

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

    def __add_to_stack__(self, function_factory):
        self.__stack__.append(function_factory)

    def do(self, func, chain=False):
        """
        This method adds a function to apply to the execution stack.

        :param func: method to be included in the execution stack
        :param chain: The results are merged into a single dataset. For example if you read multiple
                      files the results are merged to loop like a single list.
        :type chain: bool, optional

        """
        self.__add_to_stack__(
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

        :param func: method to be included in the execution stack. It must be a function that returns a boolean value,
                     otherwise the filter is not applied.
        """
        self.__stack__.append(
            type("FunctionFactory", (object,), {"type": "filter", "fuction": func})
        )
        return self
