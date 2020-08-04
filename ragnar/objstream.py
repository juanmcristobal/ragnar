from ragnar.stream import Stream
from functools import partial
import types


class ObjStream(Stream):
    def __init__(self, value, **kwargs):
        super().__init__(value, **kwargs)
        self.is_dict_source = None
        self.skip_first = kwargs.pop("skip_first", None)
        self.columns = kwargs.pop("columns", None)

    def __applyto_func__(self, row, **kwargs):
        keys = kwargs.pop("keys", None)
        idxs = kwargs.pop("idxs", None)
        func = kwargs.pop("func", None)
        entire_object = kwargs.pop("entire_object", False)

        if isinstance(row, types.GeneratorType):
            row = list(row)
            if self.is_dict_source:
                row = row[0]

        if isinstance(row, list):
            if self.is_dict_source is None:
                self.is_dict_source = False

            for index, element in enumerate(row):
                if index in idxs:
                    if entire_object:
                        yield func(dict(zip(self.columns, row)))
                    else:
                        yield func(element)
                else:
                    yield element

        if isinstance(row, dict):
            if self.is_dict_source is None:
                self.is_dict_source = True

            for key in keys:
                if entire_object:
                    row[key] = func(row)
                else:
                    row[key] = func(row[key])
            yield row

    def __next__(self):
        try:
            if self.skip_first:
                self.__show_next__()
                self.skip_first = False

            if self.is_dict_source:
                # Internally the stream creates a list of the object so the first one is extracted
                return self.__show_next__()[0]
            elif self.is_dict_source is None:
                # The content is known after the end of the first iteration
                obj = self.__show_next__()
                if self.is_dict_source and self.columns:
                    raise Exception(
                        "For dictionary type data sources it is not allowed to add the parameter columns"
                    )
                if self.is_dict_source:
                    return obj[0]
                else:
                    return obj
            else:
                return self.__show_next__()

        except StopIteration:
            self.__rebuild__()
            raise StopIteration

    def applyto(self, fields, func, entire_object=False):
        columns = getattr(self, "columns", None)
        idxs = []
        cols = []
        if columns and isinstance(fields, list):
            for col in fields:
                idxs.append(columns.index(col))
        elif columns and isinstance(fields, str):
            idxs.append(columns.index(fields))

        elif columns is None and isinstance(fields, list):
            cols = fields
        elif columns is None and isinstance(fields, str):
            cols = [fields]

        self.do(
            partial(
                self.__applyto_func__,
                keys=cols,
                idxs=idxs,
                func=func,
                entire_object=entire_object,
            )
        )
        return self
